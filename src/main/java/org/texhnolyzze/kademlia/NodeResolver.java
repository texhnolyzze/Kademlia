package org.texhnolyzze.kademlia;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.protobuf.ByteString;

import java.util.List;

class NodeResolver extends BaseResolver<FindNodeRequest, FindNodeResponse, MinMaxPriorityQueue<KadNode>> {

    NodeResolver(KadId key, Kademlia kademlia, MinMaxPriorityQueue<KadNode> neighbours) {
        super(key, kademlia, neighbours);
    }

    @Override
    MinMaxPriorityQueue<KadNode> getResult() {
        return neighbours;
    }

    @Override
    FindNodeRequest getRequest(ByteString ownerId, ByteString key) {
        return FindNodeRequest.newBuilder().setNodeId(ownerId).setKey(key).build();
    }

    @Override
    void callMethod(KadNode node, FindNodeRequest request, BaseResolverStreamObserver<FindNodeResponse> observer) {
        node.findNode(request, observer);
    }

    @Override
    BaseResolverStreamObserver<FindNodeResponse> createObserver(KadNode node, FindNodeRequest request) {
        return new NodeStreamObserver(node, request);
    }

    private class NodeStreamObserver extends BaseResolverStreamObserver<FindNodeResponse> {

        NodeStreamObserver(KadNode node, FindNodeRequest request) {
            super(node, request);
        }

        @Override
        void process(FindNodeResponse response) {
//          empty here
        }

        @Override
        boolean proceedAfter(FindNodeResponse response) {
            return true;
        }

        @Override
        List<Node> extractNodeList(FindNodeResponse response) {
            return response.getNodesList();
        }

    }

}
