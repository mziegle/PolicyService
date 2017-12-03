package edu.hm.ziegler0;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.policyservice.Period;
import io.grpc.policyservice.Policy;
import io.grpc.policyservice.PolicyId;
import io.grpc.policyservice.PolicyServiceGrpc;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * PolicyService Client
 */
public class PolicyServiceClient {


    private final ManagedChannel channel;
    private final PolicyServiceGrpc.PolicyServiceBlockingStub blockingStub;

    /**
     * Construct client connecting to HelloWorld server at {@code host:port}.
     */
    public PolicyServiceClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build());
    }

    /**
     * Construct client for accessing RouteGuide server using the existing channel.
     */
    PolicyServiceClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = PolicyServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Shutdown channel
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Say hello to server.
     */
    public void getPolicyById(int id) {
        System.out.println("Search for Policy with id " + id + " ...");

        PolicyId policyId = PolicyId.newBuilder().setId(id).build();

        Policy policy;
        try {
            policy = blockingStub.getPolicyById(policyId);
        } catch (StatusRuntimeException e) {
            System.err.println("RPC failed: {0}" + e.getStatus());
            return;
        }

        System.out.println("Policies found for id " + id);
        System.out.println(policy.getCustomer().getId());
        System.out.println(policy.getCustomer().getFirstName());
        System.out.println(policy.getCustomer().getLastName());
        System.out.println(policy.getCustomer().getCity());
        System.out.println(policy.getContracts(0).getAmountInsured());

    }

    public void streamPoliciesByValidityDate(LocalDateTime from, LocalDateTime to){

        long fromEpochMilli = from.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long toEpochMilli = to.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        Iterator<Policy> policyIterator = blockingStub.streamPoliciesByValidityDateBetween(Period.newBuilder()
                .setFrom(fromEpochMilli).setTo(toEpochMilli).build());

        while (policyIterator.hasNext()){

            Policy policy = policyIterator.next();

            System.out.println("Policies found for period " + from.toString() + " to " + to.toString());
            System.out.println(policy.getId());
            System.out.println(policy.getCustomer().getId());
            System.out.println(policy.getCustomer().getFirstName());
            System.out.println(policy.getCustomer().getLastName());
            System.out.println(policy.getCustomer().getCity());
            System.out.println(policy.getInsurer());
            System.out.println(policy.getContracts(0).getAmountInsured());
        }
    }

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */
    public static void main(String[] args) throws Exception {

        PolicyServiceClient client = new PolicyServiceClient("localhost", 50032);
        try {

            LocalDateTime from = LocalDateTime.parse("2017-07-01T00:00:00");
            LocalDateTime to = LocalDateTime.parse("2017-07-31T00:00:00");
            client.streamPoliciesByValidityDate(from,to);

        } finally {
            client.shutdown();
        }

    }
}
