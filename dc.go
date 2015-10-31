package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/miekg/dns"
)

type Rec struct {
	//Found bool
	Data string
}

type Domain struct {
	Fqdn      string
	Timestamp time.Time
	Records   map[uint16]string // dns.TypeXX
}

func (r Domain) String() string {
	records := ""
	for key, value := range r.Records {
		records += fmt.Sprintf("\n%s:\n%s", dns.TypeToString[key], value)
	}
	return fmt.Sprintf("==========\nFQDN: %s\nTIMESTAMP: %s\nRECORD:\n%s\n^^^^^^^", r.Fqdn, r.Timestamp, records)
}

func lookup(server string, domainsIn chan Domain, domainsOut chan Domain, dnsType uint16, sendingDelay time.Duration, wg *sync.WaitGroup) {
	f, err := os.OpenFile("./ERRORS.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	i := 1
	for domain := range domainsIn {
		time.Sleep(sendingDelay)
		c := new(dns.Client)
		// c.SingleInflight = true
		m := new(dns.Msg)
		fqdn := domain.Fqdn
		m.SetQuestion(fqdn+".", dnsType)
		m.RecursionDesired = true
		//fmt.Println("Query dns.Type" + dns.TypeToString[dnsType] + " " + fqdn)
		if i%1000 == 0 {
			fmt.Printf("%d\n", i)
		}
		i++
		r, _, err := c.Exchange(m, server)
		if err != nil {
			//fmt.Println("Error when looking up "+fqdn, err)
			// n, err :=
			f.WriteString(fmt.Sprintf("ERROR: %s: %s\n", fqdn, err))
			continue
		}
		//if r.Rcode == dns.RcodeNameError {
		//	continue
		//}
		if r.Rcode != dns.RcodeSuccess {
			f.WriteString(fmt.Sprintf("%s %s\n", dns.RcodeToString[r.Rcode], fqdn))
			continue
		}
		//fmt.Println("About to loop for " + fqdn)
		text := ""
		foundAny := false
		for _, a := range r.Answer {
			if (a.Header()).Rrtype == dnsType {
				/*if mx, ok := a.(*dns.MX); ok {
					text += fmt.Sprintf("%s\n", mx.String())
				} else if aa, ok := a.(*dns.AAAA); ok {
					text += fmt.Sprintf("%s\n", aa.String())
				} else if aa, ok := a.(*dns.A); ok {
					text += fmt.Sprintf("%s\n", aa.String())
				} else if ns, ok := a.(*dns.NS); ok {
					text += fmt.Sprintf("%s\n", ns.String())
				} else if soa, ok := a.(*dns.SOA); ok {
					text += fmt.Sprintf("%s\n", soa.String())
				} else {
					text += a.Header().String()
				}*/
				//fmt.Printf("Fant %s\n", dns.TypeToString[dnsType])
				foundAny = true
			}
		}
		if foundAny {
			domain.Records[dnsType] = text
		}

		domainsOut <- domain
	}
	//fmt.Println("A domainchan closed")
	// Nothing more to write
	close(domainsOut)
	wg.Done()
}

func tilDB(domainsIn chan Domain, wg *sync.WaitGroup) {
	defer wg.Done()
	DBUrl := "postgresql:///hildenae?host=/var/run/postgresql"

	/*
	   CREATE TABLE IF NOT EXISTS norid
	   ( fqdn VARCHAR(150) NOT NULL,
	     recordType VARCHAR(10),
	     data TEXT,
	     ts TIMESTAMPTZ DEFAULT NOW(),
	     PRIMARY KEY (fqdn, recordType)
	     );

	   DROP TABLE norid;
	*/

	db, err := sqlx.Connect("postgres", DBUrl)
	if err != nil {
		log.Fatalln(err)
	}
	for domain := range domainsIn {
		// Fqdn      string
		// Records   map[uint16]string // dns.TypeXX
		if len(domain.Records) > 0 {
			tx := db.MustBegin()
			for key, value := range domain.Records {
				_, err := tx.Exec("INSERT INTO norid (fqdn, recordType, data) VALUES ($1, $2, $3);", domain.Fqdn, dns.TypeToString[key], value)
				// _ = Result -> LastInsertId() (int64, error), RowsAffected() (int64, error)
				if err != nil {
					// panic: pq: duplicate key value violates unique constraint "norid_pkey"
					fmt.Printf("%T %s", err, err)
				}
			}
			err = tx.Commit()
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func tilStdout(domainsIn chan Domain, wg *sync.WaitGroup) {
	defer wg.Done()
	for domain := range domainsIn {
		if len(domain.Records) > 0 {
			fmt.Printf("SAVE TO DB %s\n", domain.Fqdn)
			//	for key, value := range domain.Records {
			//		fmt.Printf("SAVE TO DB %s: dns.Type%s\n%s\n", domain.Fqdn, dns.TypeToString[key], value)

			/*for key, value := range domain.Records {
				records += fmt.Sprintf("\n%s:\n%s", dns.TypeToString[key], value.Data)
			}*/
			// save to DB
			//	}
		}
	}
}

func tilFile(domainsIn chan Domain, wg *sync.WaitGroup) {
	defer wg.Done()
	f, err := os.OpenFile("./OUT.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	for domain := range domainsIn {
		if len(domain.Records) > 0 {
			//fmt.Printf("SAVE TO DB %s\n", domain.Fqdn)
			for key, _ := range domain.Records {
				f.WriteString(fmt.Sprintf("%s %s\n", dns.TypeToString[key], domain.Fqdn))
			}
		}
	}
}

func readFQDNs(fqdns chan Domain, wg *sync.WaitGroup) {
	defer wg.Done()
	file, err := os.Open("./input.domains")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		fqdn := scanner.Text()
		//fmt.Println(fqdn)

		fqdns <- Domain{Fqdn: fqdn, Records: make(map[uint16]string)}
	}
	close(fqdns)
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	buffersize := 100
	lookup1 := make(chan Domain, buffersize)
	// lookup2 := make(chan Domain, buffersize)
	// lookup3 := make(chan Domain, buffersize)
	// lookup4 := make(chan Domain, buffersize)
	saveRes := make(chan Domain, buffersize)

	packetsPerSeconds := 50
	sendingDelay := time.Duration(1000000000/packetsPerSeconds) * time.Nanosecond

	var wg sync.WaitGroup
	wg.Add(3)

	go lookup("8.8.8.8:53", lookup1, saveRes, dns.TypeNS, sendingDelay, &wg)
	// go lookup("8.8.8.8:53", lookup2, resultsToDB, dns.TypeNS, &wg)
	go tilFile(saveRes, &wg)
	go readFQDNs(lookup1, &wg)

	wg.Wait()
	fmt.Println("All domains checked")
}
