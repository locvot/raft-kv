// Command enginecli is a manual debug REPL for engine.Engine — it talks
// directly to an in-process MutexMap, no server/network involved. It's a
// throwaway tool for eyeballing that M1 works, not part of the M0–M8
// milestone chain (that's cmd/server and cmd/client, wired up in M4 once
// there's a real cluster to talk to).
//
// Usage: go run ./cmd/enginecli
//
//	put <key> <value>
//	get <key>
//	delete <key>
//	quit
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/locvth/mini-kv/engine"
)

func main() {
	e := engine.NewMutexMap()
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("enginecli — commands: put <key> <value> | get <key> | delete <key> | quit")
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}

		switch fields[0] {
		case "put":
			if len(fields) < 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			key := fields[1]
			value := strings.Join(fields[2:], " ")
			e.Put(key, []byte(value))
			fmt.Printf("OK\n")

		case "get":
			if len(fields) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			v, ok := e.Get(fields[1])
			if !ok {
				fmt.Println("(not found)")
				continue
			}
			fmt.Printf("%q\n", string(v))

		case "delete":
			if len(fields) != 2 {
				fmt.Println("usage: delete <key>")
				continue
			}
			e.Delete(fields[1])
			fmt.Printf("OK\n")

		case "quit", "exit":
			return

		default:
			fmt.Printf("unknown command %q\n", fields[0])
		}
	}
}
