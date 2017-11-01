package edu.hm.ziegler0;

import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.partnerservice.CustomerServiceGrpc;
import io.grpc.partnerservice.Id;
import io.grpc.policyservice.Period;
import io.grpc.policyservice.Policy;
import io.grpc.policyservice.PolicyId;
import io.grpc.policyservice.PolicyServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class PolicyServiceServer {

    public static final String POLICY_SERVICE_PORT = "POLICY_SERVICE_PORT";
    public static final String CUSTOMER_SERVICE_HOST = "CUSTOMER_SERVICE_HOST";
    public static final String CUSTOMER_SERVICE_PORT = "CUSTOMER_SERVICE_PORT";
    private Server server;

    private String customerServiceHost;
    private int customerServicePort;
    private int policyServicePort;

    private CustomerServiceGrpc.CustomerServiceBlockingStub customerServiceBlockingStub = null;
    private PolicyDataSource policyDataSource = null;


    /**
     * Construct a new policy service server
     */
    PolicyServiceServer() {

        Map<String, String> environmentVariables = System.getenv();


        if (environmentVariables.containsKey(POLICY_SERVICE_PORT)) {
            policyServicePort = Integer.parseInt(environmentVariables.get(POLICY_SERVICE_PORT));
        } else {
            System.out.printf("did not find property for policy service port");
            return;
        }

        if (environmentVariables.containsKey(CUSTOMER_SERVICE_HOST)) {
            customerServiceHost = environmentVariables.get(CUSTOMER_SERVICE_HOST);
        } else {
            System.out.printf("did not find property for policy service host");
            return;
        }

        if (environmentVariables.containsKey(CUSTOMER_SERVICE_PORT)) {
            customerServicePort = Integer.parseInt(environmentVariables.get(CUSTOMER_SERVICE_PORT));
        } else {
            System.out.printf("did not find property for customer service port");
            return;
        }

        customerServiceBlockingStub = CustomerServiceGrpc
                .newBlockingStub(ManagedChannelBuilder
                        .forAddress(customerServiceHost, customerServicePort)
                        .usePlaintext(true)
                        .build());

        policyDataSource = new PolicyDataSource();
        policyDataSource.initialize();
    }

    /**
     * start the server an register service functions
     *
     * @throws IOException
     */
    private void start() throws IOException {

        server = ServerBuilder.forPort(policyServicePort)
                .addService(new PolicyServiceGrpc.PolicyServiceImplBase() {

                    @Override
                    public void streamPoliciesByValidityDateBetween(Period request, StreamObserver<Policy> responseObserver) {

                        System.out.println(Thread.currentThread().getName());

                        final Date from = new Date(request.getFrom());
                        final Date to = new Date(request.getTo());

                        final List<Policy.Builder> policies = policyDataSource.getPoliciesByValidityDateBetween(from, to);

                        for (Policy.Builder policy : policies) {
                            policy.setCustomer(customerServiceBlockingStub.getCustomerById(
                                    Id.newBuilder().setId(policy.getCustomer().getId()).build()));
                            responseObserver.onNext(policy.build());
                        }

                        responseObserver.onCompleted();
                    }

                    @Override
                    public void getPolicyById(PolicyId request, StreamObserver<Policy> responseObserver) {

                        System.out.println(Thread.currentThread().getName());

                        final Policy.Builder policy = policyDataSource.getPolicyById(request.getId());
                        policy.setCustomer(customerServiceBlockingStub.getCustomerById(Id.newBuilder().setId(policy.getCustomer().getId()).build()));

                        responseObserver.onNext(policy.build());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();

        System.out.println("Server started, listening on " + policyServicePort);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {

                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                PolicyServiceServer.this.stop();
                System.err.println("*** server shut down");

            }
        });
    }

    private void stop() {
        if (server != null) {
            try {
                policyDataSource.close();
            } catch (IOException e) {
                System.err.println("Failed to close the connection pool");
                e.printStackTrace();
            }
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final PolicyServiceServer server = new PolicyServiceServer();
        server.start();
        server.blockUntilShutdown();
    }

}