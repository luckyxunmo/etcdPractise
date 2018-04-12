package election

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

func ExampleElection_Campaign() {
       endpoints := []string{"http://127.0.0.1:2379"}
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// create two separate sessions for election competition
	s1, err := concurrency.NewSession(cli)
	if err != nil {
		log.Fatal(err)
	}
	defer s1.Close()
	e1 := concurrency.NewElection(s1, "/my-election/")
        
       fmt.Println("s1 id is",s1.Lease(),"e1 ",e1.Rev())
	s2, err := concurrency.NewSession(cli)
	if err != nil {
		log.Fatal(err)
	}
	defer s2.Close()
	e2 := concurrency.NewElection(s2, "/my-election/")

       fmt.Println("s2 id is",s2.Lease(),"e2 ",e2.Rev())
	// create competing candidates, with e1 initially losing to e2
	var wg sync.WaitGroup
	wg.Add(2)
	electc := make(chan *concurrency.Election, 2)
	go func() {
		defer wg.Done()
		// delay candidacy so e2 wins first
		time.Sleep(3 * time.Second)
		if err := e1.Campaign(context.Background(), "e1"); err != nil {
			log.Fatal(err)
		}
               fmt.Println("e1 e1 rev ",e1.Rev())
               fmt.Println("e1 e2 rev ",e2.Rev())
		electc <- e1
	}()
	go func() {
		defer wg.Done()
		if err := e2.Campaign(context.Background(), "e2"); err != nil {
			log.Fatal(err)
		}
              fmt.Println("e2 rev ",e2.Rev())
               fmt.Println("e1 rev ",e1.Rev())
		electc <- e2
	}()

	cctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	e := <-electc
	fmt.Println("completed first election with", string((<-e.Observe(cctx)).Kvs[0].Value))

	// resign so next candidate can be elected
	if err := e.Resign(context.TODO()); err != nil {
		log.Fatal(err)
	}

	e = <-electc
	fmt.Println("completed second election with", string((<-e.Observe(cctx)).Kvs[0].Value))

 // resign so next candidate can be elected
        if err := e.Resign(context.TODO()); err != nil {
                log.Fatal(err)
        }

        e = <-electc
        fmt.Println("completed third  election with", string((<-e.Observe(cctx)).Kvs[0].Value))

	wg.Wait()

	// Output:
	// completed first election with e2
	// completed second election with e1
}
