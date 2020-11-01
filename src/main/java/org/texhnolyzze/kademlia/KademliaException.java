package org.texhnolyzze.kademlia;

public class KademliaException extends RuntimeException {

    KademliaException(String message) {
        super(message);
    }

    KademliaException(String message, Throwable cause) {
        super(message, cause);
    }

}
