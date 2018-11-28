package hashring

import (
   "crypto/md5"
   "fmt"
   "log"
   "math/rand"
)

const nodeReps = 4

type hashKey uint64

type Node struct {
   name        string
   hash        hashKey
   store       map[hashKey]string
   prev        *Node
   next        *Node
}

type HashRing struct {
   numNodes    uint
   start       *Node
   store       map[hashKey]map[hashKey]string
}

func (hr *HashRing) MakeHashRing() {
   hr.numNodes = 0
}

// add a node to the ring
func (hr *HashRing) AddNode(name string) {
   hashes := getHashes()
   for _, hash := range hashes {
      newNode := Node {
         name: name,
         hash: hash,
         store: make(map[hashKey]string),
      }
      hr.insertNode(newNode)
      hr.numNodes++
   }
}

// deletes a node from the ring
func (hr *HashRing) DeleteNode(name string) {
   // case of empty list
   if hr.numNodes == 0 {
      return
   }

   // case of a single node
   if hr.numNodes == 1 {
      if hr.start.name == name {
         hr.start = nil
         hr.numNodes--
      }
      return
   }

   // cycle through ring
   cur := hr.start
   for {
      // check whether node should be deleted
      if cur.name == name {
         // delete the last node
         if hr.numNodes == 1 {
            hr.start = nil
            hr.numNodes--
            return
         }

         // reassign deleted node's key-values
         for k, v := range cur.store {
            cur.next.store[k] = v
            delete(cur.store, k)
         }
         delete(hr.store, cur.hash)

         // unlink node from ring
         prev := cur.prev
         next := cur.next
         prev.next = next
         next.prev = prev

         // increment start if necessary
         if cur == hr.start {
            hr.start = next
            hr.numNodes--
            cur = next
            continue
         }

         hr.numNodes--
      }

      cur = cur.next

      // break when we're back at the start
      if cur == hr.start {
         break
      }
   }

   hr.PrintRing()
}

// stores the given key-value pair in the ring
func (hr *HashRing) Put(key string, val string) {
   keeper := hr.Get(key)
   hash := getHashKey(key)
   keeper.store[hash] = val
   fmt.Printf("\tkey-value { %s (0x%16X), %s }\n\tassigned to node %s\n",
         key, hash, val, keeper.Strep())
}

// returns the socket address of the server
// responsible for storing the given key
func (hr *HashRing) Get(key string) *Node {
   hash := getHashKey(key)

   cur := hr.start
   for cur.hash < hash {
      cur = cur.next
      if cur == hr.start {
         break
      }
   }

   return cur
}

// insert node into cyclic linked list
func (hr *HashRing) insertNode(n Node) {
   // case of empty list
   if hr.numNodes == 0 {
      n.prev = &n
      n.next = &n
      hr.start = &n
      return
   }

   // find node with next highest hash
   cur := hr.start
   for cur.hash < n.hash {
      cur = cur.next
      if cur == hr.start {
         break
      }
   }

   // link node to list
   n.prev = cur.prev
   n.next = cur
   n.prev.next = &n
   n.next.prev = &n

   // set start to be new node if it has the smallest hash
   if cur == hr.start && n.hash < cur.hash {
      hr.start = &n
   }

   // reassign key-value pairs to new node
   giver := n.next.store
   for k, v := range giver {
      if k <= n.hash {
         n.store[k] = v
         delete(giver, k)
      }
   }
}

func (n *Node) Strep() string {
   return fmt.Sprintf("{ name: %s, hash: 0x%16X }", n.name, n.hash)
}

func getHashes() [nodeReps]hashKey {
   var hashes [nodeReps]hashKey

   for i := range hashes {
      hashes[i] = hashKey(rand.Uint64())
   }

   return hashes
}

// reassigns key-value pairs to a newly added node
// from the next node when the key is less than the new node's
func (hr *HashRing) newReassign(n *Node) {
   giver := hr.store[n.next.hash]

   for k, v := range giver {
      if k < n.hash {
         hr.store[n.hash][k] = v
         delete(giver, k)
      }
   }
}

func (hr *HashRing) PrintRing() {
   fmt.Println("Nodes in the ring:", hr.numNodes)
   if hr.numNodes == 0 {
      return
   }

   cur := hr.start
   for {
      fmt.Printf("\t%s\n", cur.Strep())
      for k, v := range cur.store {
         fmt.Printf("\t\t{ 0x%16X, %s }\n", k, v)
      }

      cur = cur.next

      if cur == hr.start {
         break
      }
   }
   fmt.Println()
}

// generates a 128-bit MD5 hash for key
// and truncates it into a 64-bit unsigned
func getHashKey(key string) hashKey {
   checksum := md5.Sum([]byte(key))
   hash := hashKey(0)

   for i := uint(0); i < 8; i++ {
      hash |= hashKey(checksum[i]) << (i * 8)
   }

   return hash
}

func checkError(err error) {
   if err != nil {
      log.Fatal(err)
   }
}
