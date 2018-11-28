package main

import (
   "bufio"
   "fmt"
   "log"
   "os"
   "strings"

   "github.com/danemortensen/hashring"
)

var (
   hr hashring.HashRing
)

func checkError(err error) {
   if err != nil {
      log.Fatal(err)
   }
}

func instruct() {
   fmt.Println("Usage:")
   fmt.Println("\tAdd node: add <name>")
   fmt.Println("\tDelete node: delete <name>")
   fmt.Println("\tStore key-value: put <key> <value>")
   fmt.Println("\tGet node storing key-value: get <key>")
   fmt.Println("\tPrint ring: print")
   fmt.Println("\tHelp: help")
   fmt.Println("\tQuit: quit")
   fmt.Println()
}

func interpret(input string) {
   words := strings.Split(input, " ")

   switch words[0] {
   case "add":
      hr.AddNode(words[1])
      fmt.Println("Node", words[1], "added")
      hr.PrintRing()
   case "delete":
      hr.DeleteNode(words[1])
      hr.PrintRing()
   case "put":
      hr.Put(words[1], words[2])
      hr.PrintRing()
   case "get":
      fmt.Println(fmt.Sprintf("\t%s", hr.Get(words[1]).Strep()))
   case "print":
      hr.PrintRing()
   case "help":
      instruct()
   default:
      fmt.Println("Invalid input")
   }
}

func main() {
   instruct()
   scanner := bufio.NewScanner(os.Stdin)
   for {
      fmt.Printf("Enter a command: ")
      scanner.Scan()
      input := scanner.Text()
      if input == "quit" {
         break
      }
      interpret(input)
   }
   checkError(scanner.Err())
}
