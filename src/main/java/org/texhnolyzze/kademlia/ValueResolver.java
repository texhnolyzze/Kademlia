package org.texhnolyzze.kademlia;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

class ValueResolver extends BaseResolver<FindValueRequest, FindValueResponse, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(ValueResolver.class);

    private final Set<ByteString> values = new HashSet<>();
    private KadNode nearestWithoutValue;
    private byte[] nearestWithoutValueDist = new byte[KadId.SIZE_BYTES];
    private byte[] tempDist = new byte[KadId.SIZE_BYTES];

    ValueResolver(KadId key, Kademlia kademlia, MinMaxPriorityQueue<KadNode> neighbours) {
        super(key, kademlia, neighbours);
    }

    @Override
    byte[] getResult() {
        if (values.isEmpty()) {
            LOG.warn("No value found for key {}", key);
            return null;
        }
        if (values.size() > 1)
            LOG.warn("More than one value found for key {}", key);
        return values.iterator().next().toByteArray();
    }

    @Override
    byte[] resolve() {
        byte[] result = super.resolve();
        if (result != null && nearestWithoutValue != null) {
            nearestWithoutValue.store(
                kademlia.getStoreRequestBuilder().
                    setKey(key.asByteString()).
                    setVal(ByteString.copyFrom(result)).
                    build()
            );
        }
        return result;
    }

    @Override
    FindValueRequest getRequest(ByteString key) {
        return kademlia.getFindValueRequestBuilder().setKey(key).build();
    }

    @Override
    void callMethod(KadNode node, FindValueRequest request, BaseResolverStreamObserver<FindValueResponse> observer) {
        node.findValue(request, observer);
    }

    @Override
    BaseResolverStreamObserver<FindValueResponse> createObserver(KadNode node, FindValueRequest request) {
        return new ValueObserver(node, request);
    }

    private class ValueObserver extends BaseResolverStreamObserver<FindValueResponse> {

        ValueObserver(KadNode node, FindValueRequest findValueRequest) {
            super(node, findValueRequest);
        }

        @Override
        void process(FindValueResponse response) {
            if (!response.getVal().isEmpty()) {
                values.add(response.getVal());
            } else {
                if (nearestWithoutValue == null) {
                    nearestWithoutValue = node;
                    nearestWithoutValue.getId().distanceTo(key, nearestWithoutValueDist);
                } else {
                    node.getId().distanceTo(key, tempDist);
                    if (KadId.compare(tempDist, nearestWithoutValueDist) < 0) {
                        nearestWithoutValue = node;
                        byte[] temp = nearestWithoutValueDist;
                        nearestWithoutValueDist = tempDist;
                        tempDist = temp;
                    }
                }
            }
        }

        @Override
        boolean proceedAfter(FindValueResponse response) {
            return values.isEmpty() && response.getVal() == null;
        }

        @Override
        List<Node> extractNodeList(FindValueResponse response) {
            return response.getNodesList();
        }

    }

}
