package core

import (
	"container/list"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"os"
	"os/signal"
	"path"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	ipldgit "github.com/ipfs/go-ipld-git"
	"github.com/ipld/go-ipld-prime"
	"github.com/remeh/sizedwaitgroup"
	"github.com/rs/zerolog/log"

	ipldgitprime "github.com/drgomesp/go-ipld-gitprime"
)

type Push struct {
	objectDir string
	gitDir    string

	done    uint64
	todoc   uint64
	todo    *list.List
	log     *stdlog.Logger
	tracker *Tracker
	repo    *git.Repository
	store   ipldgitprime.Store

	processing map[string]int
	subs       map[string][][]byte

	errCh   chan error
	wg      sizedwaitgroup.SizedWaitGroup
	linkSys *ipld.LinkSystem

	NewNode func(context.Context, string, []byte) error
}

func NewPush(gitDir string, tracker *Tracker, ls *ipld.LinkSystem, repo *git.Repository, st ipldgitprime.Store) *Push {
	return &Push{
		store:      st,
		objectDir:  path.Join(gitDir, "objects"),
		gitDir:     gitDir,
		todo:       list.New(),
		log:        stdlog.New(os.Stderr, "push: ", 0),
		tracker:    tracker,
		repo:       repo,
		todoc:      1,
		processing: map[string]int{},
		subs:       map[string][][]byte{},
		linkSys:    ls,
		wg:         sizedwaitgroup.New(512),
		errCh:      make(chan error),
	}
}

func (p *Push) PushHash(hash string) error {
	p.todo.PushFront(hash)
	return p.doWork()
}

func (p *Push) doWork() error {
	defer p.wg.Wait()

	intch := make(chan os.Signal, 1)
	signal.Notify(intch, os.Interrupt)
	go func() {
		<-intch
		p.errCh <- errors.New("interrupted")
	}()
	defer signal.Stop(intch)

	for e := p.todo.Front(); e != nil; e = e.Next() {
		context.Background()
		ctx := context.Background()
		if df, ok := e.Value.(func() error); ok {
			if err := df(); err != nil {
				return err
			}
			p.todoc--
			continue
		}

		hash := e.Value.(string)

		sha, err := hex.DecodeString(hash)
		if err != nil {
			return fmt.Errorf("push: %v", err)
		}

		_, processing := p.processing[string(sha)]
		if processing {
			p.todoc--
			continue
		}

		//has, err := p.tracker.HasEntry(sha)
		//if err != nil {
		//	return fmt.Errorf("push/process: %v", err)
		//}

		var has bool
		if has {
			p.todoc--
			continue
		}

		expectedCid, err := CidFromHex(hash)
		if err != nil {
			return err
		}

		obj, err := p.repo.Storer.EncodedObject(plumbing.AnyObject, plumbing.NewHash(hash))
		if err != nil {
			return fmt.Errorf("push/getObject(%s): %v", hash, err)
		}
		spew.Dump(obj)

		rawReader, err := obj.Reader()
		if err != nil {
			return fmt.Errorf("push: %v", err)
		}

		raw, err := io.ReadAll(rawReader)
		if err != nil {
			return err
		}

		switch obj.Type() {
		case plumbing.CommitObject:
			raw = append([]byte(fmt.Sprintf("commit %d\x00", obj.Size())), raw...)
		case plumbing.TreeObject:
			raw = append([]byte(fmt.Sprintf("tree %d\x00", obj.Size())), raw...)
		case plumbing.BlobObject:
			raw = append([]byte(fmt.Sprintf("blob %d\x00", obj.Size())), raw...)
		case plumbing.TagObject:
			raw = append([]byte(fmt.Sprintf("tag %d\x00", obj.Size())), raw...)
		}

		p.done++
		if p.done%100 == 0 || p.done == p.todoc {
			p.log.Printf("%d/%d (P:%d) %s %s\r\x1b[A", p.done, p.todoc, len(p.processing), hash, expectedCid.String())
		}

		p.wg.Add()
		go func() {
			defer p.wg.Done()

			if p.NewNode != nil {
				log.Trace().Msgf("NewNode(%s, %s)", hash, string(raw))
				if err := p.NewNode(ctx, hash, raw); err != nil {
					p.errCh <- err
					return
				}
			}
		}()

		nd, err := ipldgit.ParseObjectFromBuffer(raw)
		if err != nil {
			return err
		}

		n, err := p.processLinks(nd.(ipldgit.Commit), []byte(hash))
		if err != nil {
			return err
		}

		if n == 0 {
			p.todoc++
			p.todo.PushBack(p.doneFunc(sha))
		} else {
			p.processing[string(sha)] = n
		}

		select {
		case e := <-p.errCh:
			return e
		default:
		}
	}

	p.log.Printf("arriba\n")
	return nil
}

func (p *Push) doneFunc(sha []byte) func() error {
	return func() error {
		//if err := p.tracker.AddEntry(sha); err != nil {
		//	return err
		//}
		delete(p.processing, string(sha))

		for _, sub := range p.subs[string(sha)] {
			p.processing[string(sub)]--
			if p.processing[string(sub)] <= 0 {
				p.todoc++
				p.todo.PushBack(p.doneFunc(sub))
			}
		}
		delete(p.subs, string(sha))
		return nil
	}
}

func (p *Push) processLinks(commit ipldgit.Commit, selfSha []byte) (int, error) {
	mi := commit.MapIterator()

	var count int
	for !mi.Done() {
		k, _, err := mi.Next()
		if err != nil {
			log.Fatal().Err(err).Send()
		}

		key, err := k.AsString()
		if err != nil {
			return -1, err
		}

		if key == "tree" {
			spew.Dump(key)
			if _, proc := p.processing[key]; !proc {
				//has, err := p.tracker.HasEntry([]byte(key))
				//if err != nil {
				//	return -1, err
				//}
				//
				//if has {
				//	continue
				//}
			}

			p.subs[string(selfSha)] = append(p.subs[string(selfSha)], selfSha)

			count++
			p.todoc++
			p.todo.PushBack(string(selfSha))
		}
	}

	return count, nil
}
