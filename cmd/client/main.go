// Command client is a small CLI against a RaftKV cluster, following
// leader redirects automatically (see transport.Client). -peers has the
// same local default as cmd/server, so plain `go run ./cmd/client` talks
// to a cluster started with cmd/server's own defaults.
//
// With no KEY/VALUE arguments it's an interactive REPL, same shape as
// cmd/storagecli/cmd/enginecli:
//
//	go run ./cmd/client
//	> put hello world
//	OK
//	> get hello
//	"world"
//	> quit
//
// A command line still runs one-shot, for scripting:
//
//	go run ./cmd/client put hello world
//	go run ./cmd/client get hello
//	go run ./cmd/client delete hello
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/locvth/mini-kv/transport"
)

const defaultPeers = "localhost:9001,localhost:9002,localhost:9003"

func main() {
	peersFlag := flag.String("peers", defaultPeers, "comma-separated host:port for every peer")
	timeout := flag.Duration("timeout", 5*time.Second, "per-command deadline")
	flag.Parse()

	peers := strings.Split(*peersFlag, ",")
	client := transport.NewClient(peers)
	defer client.Close()

	args := flag.Args()
	if len(args) == 0 {
		repl(client, *timeout, peers)
		return
	}
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: client [-peers=host:port,...] <get|put|delete> KEY [VALUE]")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := run(ctx, client, args[0], args[1], args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run executes one get/put/delete and prints its result the same way
// either caller (repl or the one-shot command-line form) needs.
func run(ctx context.Context, client *transport.Client, cmd, key string, rest []string) error {
	switch cmd {
	case "get":
		value, ok, err := client.Get(ctx, key)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Println("(not found)")
			return nil
		}
		fmt.Printf("%q\n", string(value))

	case "put":
		if len(rest) < 1 {
			fmt.Println("usage: put <key> <value>")
			return nil
		}
		value := strings.Join(rest, " ")
		if err := client.Put(ctx, key, []byte(value)); err != nil {
			return err
		}
		fmt.Println("OK")

	case "delete":
		if err := client.Delete(ctx, key); err != nil {
			return err
		}
		fmt.Println("OK")

	default:
		fmt.Printf("unknown command %q\n", cmd)
	}
	return nil
}

func repl(client *transport.Client, timeout time.Duration, peers []string) {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Printf("client — peers: %s\n", strings.Join(peers, ","))
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
		if fields[0] == "quit" || fields[0] == "exit" {
			return
		}
		if len(fields) < 2 {
			fmt.Printf("usage: %s <key> [value]\n", fields[0])
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := run(ctx, client, fields[0], fields[1], fields[2:])
		cancel()
		if err != nil {
			fmt.Println("error:", err)
		}
	}
}
