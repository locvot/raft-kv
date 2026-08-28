package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/locvth/mini-kv/storage"
)

func main() {
	dir := flag.String("dir", "tmp/storagecli-data", "directory to store WAL/SSTable/MANIFEST files in (reused across runs)")
	flushThreshold := flag.Int("flushthreshold", 0, "memtable size in bytes that triggers a flush (0 = default 4MiB); set small (e.g. 64) to watch flush/compaction happen live")
	flag.Parse()

	s, err := storage.Open(*dir, *flushThreshold)
	if err != nil {
		fmt.Fprintf(os.Stderr, "storage.Open(%q): %v\n", *dir, err)
		os.Exit(1)
	}
	defer s.Close()

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Printf("storagecli — data dir: %s\n", *dir)
	fmt.Println("commands: put <key> <value> | get <key> | delete <key> | quit")
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
			if err := s.Put(key, []byte(value)); err != nil {
				fmt.Printf("error: %v\n", err)
				continue
			}
			fmt.Println("OK")

		case "get":
			if len(fields) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			v, ok, err := s.Get(fields[1])
			if err != nil {
				fmt.Printf("error: %v\n", err)
				continue
			}
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
			if err := s.Delete(fields[1]); err != nil {
				fmt.Printf("error: %v\n", err)
				continue
			}
			fmt.Println("OK")

		case "quit", "exit":
			return

		default:
			fmt.Printf("unknown command %q\n", fields[0])
		}
	}
}
