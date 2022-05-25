package main

/*
	Simple DNS Server implemented in Go
	BSD 2-Clause License
	Copyright (c) 2019, Daniel Lorch
	All rights reserved.
	Redistribution and use in source and binary forms, with or without
	modification, are permitted provided that the following conditions are met:
	1. Redistributions of source code must retain the above copyright notice, this
	   list of conditions and the following disclaimer.
	2. Redistributions in binary form must reproduce the above copyright notice,
       this list of conditions and the following disclaimer in the documentation
       and/or other materials provided with the distribution.
	THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
	AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
	IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
	DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
	FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
	DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
	SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
	CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
	OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
	OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

import (
	"sync"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
)

func handle(rw dns.ResponseWriter, queryMsg *dns.Msg) {
	log.Infof("received request from %v", rw.RemoteAddr())

	/**
	 * lookup values
	 */
	output := make(chan TypedResourceRecord)

	// Look up queries in parallel.
	go func(output chan<- TypedResourceRecord) {
		var wg sync.WaitGroup
		for _, query := range queryMsg.Question {
			wg.Add(1)
			go Lookup(&wg, query, output, "")
		}
		wg.Wait()
		close(output)
	}(output)

	// Collect output from each query.
	var answerResourceRecords []dns.RR
	var authorityResourceRecords []dns.RR
	var additionalResourceRecords []dns.RR
	for rec := range output {
		switch rec.Type {
		case ResourceAnswer:
			answerResourceRecords = append(answerResourceRecords, rec.Record)
		case ResourceAuthority:
			authorityResourceRecords = append(authorityResourceRecords, rec.Record)
		case ResourceAdditional:
			additionalResourceRecords = append(additionalResourceRecords, rec.Record)
		}
	}

	/**
	 * write response
	 */
	response := new(dns.Msg)
	response.SetReply(queryMsg)
	response.RecursionAvailable = true

	for _, answerResourceRecord := range answerResourceRecords {
		response.Answer = append(response.Answer, answerResourceRecord)
	}

	for _, authorityResourceRecord := range authorityResourceRecords {
		response.Ns = append(response.Ns, authorityResourceRecord)
	}

	for _, additionalResourceRecord := range additionalResourceRecords {
		response.Extra = append(response.Extra, additionalResourceRecord)
	}

	err := rw.WriteMsg(response)
	if err != nil {
		log.Errorf("error writing response %v", err)
	}
}

func main() {
	InitDB()

	s := &dns.Server{
		Addr:    ":1053",
		Net:     "udp",
		Handler: dns.HandlerFunc(handle),
	}
	err := s.ListenAndServe()
	if err != nil {
		log.Fatalf("Error resolving UDP address: %v", err)
	}
}
