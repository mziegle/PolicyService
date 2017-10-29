package edu.hm.ziegler0;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import io.grpc.partnerservice.Customer;
import io.grpc.policyservice.Policy;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.naming.InitialContext;
import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.Date;

public class DatabaseLoader implements Closeable {

    public static final String SQL_POLICY_BY_ID = "select * from policy inner join contract on " +
                                                    "policy.id = contract.policy_id where policy.id = ?";

    public static final String SQL_POLICY_VALIDITY_DATE_BETWEEN = "select * from policy inner join contract on " +
            "policy.id = contract.policy_id where policy.validity_date between ? and ?";

    public static final String DB_SERVER = "DB_SERVER";
    public static final String DB_NAME = "DB_NAME";
    public static final String DB_USER = "DB_USER";
    public static final String DB_PASSWORD = "DB_PASSWORD";

    private BasicDataSource dataSource;

    /**
     * Initializes the class DatabaseLoader. Tries to read the environment variables needed.
     * Establishes a connection to the database.
     */
    public boolean initialize(){

        Map<String, String> environmentVariables = System.getenv();

        boolean allEnvironmentVariablesFound = true;

        String db_server = null;
        String db_name = null;
        String db_user = null;
        String db_password = null;

        if(environmentVariables.containsKey(DB_SERVER)){
            db_server = environmentVariables.get(DB_SERVER);
        } else {
            System.out.printf("did not find property for database server");
            allEnvironmentVariablesFound = false;
        }

        if(environmentVariables.containsKey(DB_SERVER)){
            db_name = environmentVariables.get(DB_NAME);
        } else {
            System.out.printf("did not find property for database name");
            allEnvironmentVariablesFound = false;
        }

        if(environmentVariables.containsKey(DB_USER)){
            db_user = environmentVariables.get(DB_USER);
        } else {
            System.out.printf("did not find property for database user");
            allEnvironmentVariablesFound = false;
        }

        if(environmentVariables.containsKey(DB_PASSWORD)){
            db_password = environmentVariables.get(DB_PASSWORD);
        } else {
            System.out.printf("did not find property for database password");
            allEnvironmentVariablesFound = false;
        }

        connect(db_server,db_name,db_user,db_password);

        return  allEnvironmentVariablesFound;
    }

    /**
     * Establish a connection to the database
     */
    private void connect(final String db_server,final String db_name,final String db_user,final String db_password) {

        try {
            dataSource = new BasicDataSource();
            String url = "jdbc:mysql://" +
                    db_server + "/"
                    + db_name + "?autoReconnect=true" +
                    "&useSSL=false";
            dataSource.setDriver(DriverManager.getDriver(url));

            dataSource.setUsername(db_user);
            dataSource.setPassword(db_password);
            dataSource.setUrl(url);

        } catch (SQLException e) {
            System.err.println("Establishing a connection to the database has failed");
            e.printStackTrace();
        }
    }

    /**
     *
     * @param from
     * @param to
     * @return
     */
    public List<Policy.Builder> getPoliciesByValidityDateBetween(final Date from, final Date to) {

        List<Policy.Builder> policies = new ArrayList<>();

        try (Connection connection = dataSource.getConnection()) {

            try(PreparedStatement preparedStatement = connection.prepareStatement(SQL_POLICY_VALIDITY_DATE_BETWEEN)) {

                LocalDate ldFrom = Instant.ofEpochMilli(from.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
                LocalDate ldTo = Instant.ofEpochMilli(from.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();

                preparedStatement.setDate(1, java.sql.Date.valueOf(ldFrom));
                preparedStatement.setDate(2, java.sql.Date.valueOf(ldTo));

                try (ResultSet resultSet = preparedStatement.executeQuery()) {

                    int lastPolicyId = -1;
                    Policy.Builder policy = null;

                    while (resultSet.next()) {

                        int policyId = resultSet.getInt("policy.id");
                        if(policyId != lastPolicyId){
                            policy = Policy.newBuilder();
                        }

                        policy
                                .setId(policyId)
                                .setCustomer(Customer.newBuilder().setId(resultSet.getInt("policy.customer_id")))
                                .setValidityDate(resultSet.getDate("policy.validity_date").getTime())
                                .setTerritorialScope(resultSet.getString("policy.territorial_scope"))
                                .setInsurer(resultSet.getString("policy.insurer"));

                        policy.addContractsBuilder()
                                .setId(resultSet.getInt("contract.id"))
                                .setType(resultSet.getString("contract.type"))
                                .setAmountInsured(resultSet.getDouble("contract.amount_insured"))
                                .setCompletionDate(resultSet.getDate("contract.completion_date").getTime())
                                .setExpirationDate(resultSet.getDate("contract.expiration_date").getTime())
                                .setAnnualSubscription(resultSet.getDouble("contract.annual_subscription")).build();

                        policies.add(policy);
                        lastPolicyId = policyId;

                    }
                }

            } catch (SQLException e) {
                System.err.println("executing query failed: " + SQL_POLICY_VALIDITY_DATE_BETWEEN);
                e.printStackTrace();
            }

        } catch (SQLException e) {
            System.err.println("Establishing a connection to the database has failed");
            e.printStackTrace();
        }

        return policies;
    }

    /**
     * get Policy by id
     *
     * @param id
     * @return
     */
    public Policy.Builder getPolicyById(int id) {

        Policy.Builder policyToFill = Policy.newBuilder();

        try (Connection connection = dataSource.getConnection()) {

            policyToFill.setId(id);

            try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_POLICY_BY_ID)) {

                preparedStatement.setInt(1, policyToFill.getId());

                try (ResultSet resultSet = preparedStatement.executeQuery()) {

                    while (resultSet.next()) {

                        policyToFill
                                .setId(resultSet.getInt("policy.id"))
                                .setCustomer(Customer.newBuilder().setId(resultSet.getInt("policy.customer_id")))
                                .setValidityDate(resultSet.getDate("policy.validity_date").getTime())
                                .setTerritorialScope(resultSet.getString("policy.territorial_scope"))
                                .setInsurer(resultSet.getString("policy.insurer"));

                        policyToFill.addContractsBuilder()
                                .setId(resultSet.getInt("contract.id"))
                                .setType(resultSet.getString("contract.type"))
                                .setAmountInsured(resultSet.getDouble("contract.amount_insured"))
                                .setCompletionDate(resultSet.getDate("contract.completion_date").getTime())
                                .setExpirationDate(resultSet.getDate("contract.expiration_date").getTime())
                                .setAnnualSubscription(resultSet.getDouble("contract.annual_subscription")).build();
                    }
                }

            } catch (SQLException e) {
                System.err.println("executing query failed: " + SQL_POLICY_BY_ID);
                e.printStackTrace();
            }

        } catch (SQLException e) {
            System.err.println("Establishing a connection to the database has failed");
            e.printStackTrace();
        }

        return policyToFill;
    }

    @Override
    public void close() throws IOException {
        try {
            dataSource.close();
        } catch (SQLException e) {
            System.err.println("Closing the connection to the database has failed");
            e.printStackTrace();
        }
    }
}
