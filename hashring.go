package hashring

import (
   "crypto/md5"
   "fmt"
   "sort"
)

type hashKey uint64

type node struct {
   sockAddr string
   hash hashKey
}

type HashRing struct {
   nodes map[string]hashKey
   table map[hashKey]string
}

func (hr *HashRing) MakeHashRing() {
   hr.nodes = make(map[string]hashKey)
   hr.table = make(map[hashKey]string)
}

// add a node to the ring
func (hr *HashRing) AddNode(sockAddr string) {
   key := hashTrunc(md5.Sum([]byte(sockAddr)))

   hr.nodes[sockAddr] = key

   fmt.Printf("Node added\n\tsocket address: %s\n\tkey: 0x%x\n", sockAddr, key)
   hr.table[key] = sockAddr
}

func (hr *HashRing) DeleteNode() {

}

func (hr *HashRing) Get(key string) string {
   return ""
}

func (hr *HashRing) Put(key string, val string) {

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
   if len(hr.nodes) < 2 {
      return
   }

   // sort nodes by their hashes
   var nodeHashes []hashKey
   for k := range hr.nodes {
      nodeHashes = append(nodeHashes, hr.nodes[k])
   }
   lt := func(i, j int) bool {
      return nodeHashes[i] < nodeHashes[j]
   }
   sort.Slice(nodeHashes, lt)

   newHash := hr.nodes[newNode]
   for hash := range nodeHashes {
      if hash != newHash {
         continue
      }

      if hash == 0 {

      }
   }
}
