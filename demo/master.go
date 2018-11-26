package main

import (
   "flag"
   //"fmt"
   "log"
   "net"
   "net/http"
   //"os"

   "github.com/danemortensen/hashring"
   "github.com/gorilla/mux"
)

var (
   sockAddr string
   hr hashring.HashRing
)

func checkError(err error) {
   if err != nil {
      log.Fatal(err)
   }
}


func init() {
   flag.StringVar(&sockAddr, "sa", "", "My socket address (<inetAddr>:<port>)")
}

func checkArgs() {
   if sockAddr == "" {
      log.Fatal("Socket address required")
   }
}

//func addNodes(sockAddrs string[]) {
   //for sockAddr := range sockAddrs {
      
   //}
//}

func main() {
   flag.Parse()
   checkArgs()
   hr.MakeHashRing()
   hr.AddNode("a")
   hr.AddNode("b")
   hr.AddNode("asdfasd;fkl")
   hr.AddNode(sockAddr)
   hr.Get("fuck")
   hr.Get("shit")
   hr.Get("bitch")
   listener, err := net.Listen("tcp", sockAddr)
   checkError(err)
   rtr := mux.NewRouter()
   log.Fatal(http.Serve(listener, rtr))
}
