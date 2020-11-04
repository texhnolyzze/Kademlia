This is a simple implementation of Kademlia distributed hash table. It uses gRPC (although gRPC runs over HTTP protocol, but in the original paper authors used UDP. Unfortunately I can't find any UDP RPC library for Java).

This implementation will probably not work in NAT-ed environments. 

In test files you can find [KademliaTest](src/test/java/org/texhnolyzze/kademlia/KademliaTest.java). 
It is integration test, so if you want to see this implementation in action, you can play with it, tune various parameters, etc.
In this test you can see two last lines:
```java
assertTrue(matchRatio >= 99);
assertTrue(storeRatio >= 99);
```
Although these two variables often equal to 100.0, this is not always the case (it can be anywhere from 99.4 to 100.0). 
If you know the cause of this please write me. 