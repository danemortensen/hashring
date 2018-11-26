package hashring

import (
   "crypto/md5"
   //"encoding/json"
   "fmt"
   "log"
   //"net/http"
   //"net/url"
   //"sort"
)

type hashKey uint64

type node struct {
   sockAddr string
   hash hashKey
   prev *node
   next *node
}

type HashRing struct {
   //nodes map[string]hashKey
   nodes *node
   table map[hashKey]string
}

func (hr *HashRing) MakeHashRing() {
   //hr.nodes = make(map[string]hashKey)
   hr.table = make(map[hashKey]string)
}

// add a node to the ring
func (hr *HashRing) AddNode(sockAddr string) {
   hash := hashTrunc(md5.Sum([]byte(sockAddr)))

   newNode := node {
      sockAddr: sockAddr,
      hash: hash,
   }

   fmt.Printf("Node added\n\tsocket address: %s\n\tkey: 0x%x\n",
              sockAddr, newNode.hash)
   hr.insertNode(newNode)
   hr.table[hash] = sockAddr
}

func (hr *HashRing) insertNode(n node) {
   if hr.nodes == nil {
      n.prev = &n
      n.next = &n
      hr.nodes = &n
      return
   }

   cur := hr.nodes
   for cur.hash < n.hash {
      cur = cur.next
      if cur == hr.nodes {
         break
      }
   }

   if cur == hr.nodes && cur.next != cur {
      hr.nodes = &n
   }

   n.prev = cur.prev
   printNode(n.prev)
   n.next = cur
   printNode(n.next)
   n.prev.next = &n
   n.next.prev = &n
   fmt.Println()

   hr.printNodes()
}

func printNode(n *node) {
   fmt.Printf("{ sockAddr: %s, hash: 0x%x }\n", n.sockAddr, n.hash)
}

func (hr *HashRing) printNodes() {
   cur := hr.nodes
   for {
      printNode(cur)
      cur = cur.next

      if cur == hr.nodes {
         break
      }
   }
}

func (hr *HashRing) DeleteNode() {

}

func (hr *HashRing) Get(key string) string {
   hash := getHashKey(key)
   sockAddr := hr.whichNode(hash)

   return sockAddr
}

func (hr *HashRing) Put(key string, val string) {

}

func (hr *HashRing) whichNode(hash hashKey) string {
   cur := hr.nodes
   for cur.hash < hash {
      cur = cur.next
      if cur == hr.nodes {
         break
      }
   }
   fmt.Println(cur.sockAddr)
   return cur.sockAddr
}

func getHashKey(key string) hashKey {
   return hashTrunc(md5.Sum([]byte(key)))
}

// truncates a 128-bit MD5 hash into a 64-bit unsigned
func hashTrunc(checksum [md5.Size]byte) hashKey {
   key := hashKey(0)
   for i := uint(0); i < 8; i++ {
      key |= hashKey(checksum[i]) << (i * 8)
   }
   return key
}

func (hr *HashRing) reassign(newNode string) {
   //if len(hr.nodes) < 2 {
      //return
   //}

   //// sort nodes by their hashes
   //var nodeHashes []hashKey
   //for k := range hr.nodes {
      //nodeHashes = append(nodeHashes, hr.nodes[k])
   //}
   //lt := func(i, j int) bool {
      //return nodeHashes[i] < nodeHashes[j]
   //}
   //sort.Slice(nodeHashes, lt)

   //newHash := hr.nodes[newNode]
   //for hash := range nodeHashes {
      //if hash != newHash {
         //continue
      //}

      //if hash == 0 {

      //}
   //}
}

func checkError(err error) {
   if err != nil {
      log.Fatal(err)
   }
}
