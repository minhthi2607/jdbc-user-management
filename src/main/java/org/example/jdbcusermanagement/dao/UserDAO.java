package org.example.jdbcusermanagement.dao;

import org.example.jdbcusermanagement.model.User;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class UserDAO implements IUserDAO{
    private String jdbcUrl = "jdbc:mysql://localhost:3306/demo_user?useSSL=false";
    private String jdbcUsername = "root";
    private String jdbcPassword = "123456";

    private static final String INSERT_USER_SQL = "INSERT INTO users (name, email, country) VALUES (?, ?, ?)";
    private static final String SELECT_USER_BY_ID = "SELECT id,name,email,country FROM users WHERE id = ?";
    private static final String SELECT_ALL_USERS = "SELECT * FROM users";
    private static final String DELETE_USERS_SQL = "DELETE FROM users WHERE id = ?";
    private static final String UPDATE_USERS_SQL = "UPDATE users SET name = ?, email = ?, country = ? WHERE id = ?";
    private static final String SELECT_USER_BY_COUNTRY =
            "SELECT id, name, email, country FROM users WHERE country LIKE ?";
    private static final String ORDER_BY_NAME_ASC = "SELECT * FROM users ORDER BY name ASC";
    private static final String ORDER_BY_NAME_DESC = "SELECT * FROM users ORDER BY name DESC";

    private static final String SQL_INSERT = "INSERT INTO EMPLOYEE (NAME, SALARY, CREATED_DATE) VALUES (?,?,?)";
    private static final String SQL_UPDATE = "UPDATE EMPLOYEE SET SALARY=? WHERE NAME=?";
    private static final String SQL_TABLE_CREATE = "CREATE TABLE EMPLOYEE"
            + "("
            + " ID serial,"
            + " NAME varchar(100) NOT NULL,"
            + " SALARY numeric(15, 2) NOT NULL,"
            + " CREATED_DATE timestamp,"
            + " PRIMARY KEY (ID)"
            + ")";
    private static final String SQL_TABLE_DROP = "DROP TABLE IF EXISTS EMPLOYEE";

    public UserDAO(){}

    protected Connection getConnection(){
       Connection connection = null;

       try {
           Class.forName("com.mysql.jdbc.Driver");
           connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
       }catch (SQLException e){
           e.printStackTrace();
       }catch (ClassNotFoundException e){
           e.printStackTrace();
       }
       return connection;
    }
    public void insertUser(User user) throws SQLException{
        System.out.println("Insert user sql: " + INSERT_USER_SQL);
        try(Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(INSERT_USER_SQL);){
            preparedStatement.setString(1, user.getName());
            preparedStatement.setString(2, user.getEmail());
            preparedStatement.setString(3, user.getCountry());
            System.out.println("preparedStatement: " + preparedStatement);
            preparedStatement.executeUpdate();
        }catch (SQLException  e){
            printSQLException(e);
        }
    }

    public User selectUser(int id){
        User user = null;
        try(Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(SELECT_USER_BY_ID);){
            preparedStatement.setInt(1, id);
            System.out.println("prestatement: " + preparedStatement);
            ResultSet rs = preparedStatement.executeQuery();

            while(rs.next()){
                String name = rs.getString("name");
                String email = rs.getString("email");
                String country = rs.getString("country");
                user = new User(id, name, email, country);

            }
        }
        catch (SQLException e){
            printSQLException(e);
        }
        return user;
    }

    public List<User> selectAllUsers(){
        List<User> users = new ArrayList<>();
//        try(Connection connection = getConnection(); PreparedStatement preparedStatement =  connection.prepareStatement(SELECT_ALL_USERS);){
//            System.out.println("prestatement: " + preparedStatement);
//            getUser(users, preparedStatement);
//        }catch (SQLException e){
//            printSQLException(e);
//        }
        String sql = "{Call get_all_users()}";
        try(Connection connection = getConnection(); CallableStatement callableStatement = connection.prepareCall(sql)){
            getUser(users, callableStatement);
        }catch (SQLException e){
            printSQLException(e);
        }
        return users;
    }

    public boolean deleteUser(int id) throws SQLException{
        boolean isDeleted = false;
        String sql = "{Call delete_user(?)}";
        try(Connection connection = getConnection(); CallableStatement callableStatement = connection.prepareCall(sql);){
            callableStatement.setInt(1, id);
            isDeleted = callableStatement.executeUpdate() > 0;
        }
//        try(Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(DELETE_USERS_SQL);){
//            preparedStatement.setInt(1, id);
//            isDeleted = preparedStatement.executeUpdate() > 0;
//
//        }
        return isDeleted;
    }

    public boolean updateUser(User user) throws SQLException{
        boolean isUpdated = false;
        String sql = "{Call update_user(?,?,?,?)}";
        try(Connection connection = getConnection(); CallableStatement callableStatement = connection.prepareCall(sql);){
            callableStatement.setInt(1, user.getId());
            callableStatement.setString(2, user.getName());
            callableStatement.setString(3, user.getEmail());
            callableStatement.setString(4, user.getCountry());

            isUpdated = callableStatement.executeUpdate() > 0;
        }catch (SQLException e){
            printSQLException(e);
        }
//        try(Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_USERS_SQL);){
//            preparedStatement.setString(1, user.getName());
//            preparedStatement.setString(2, user.getEmail());
//            preparedStatement.setString(3, user.getCountry());
//            preparedStatement.setInt(4, user.getId());
//            isUpdated = preparedStatement.executeUpdate() > 0;
//        }
        return isUpdated;
    }

    private void printSQLException(SQLException ex) {
        for (Throwable e : ex) {
            if (e instanceof SQLException) {
                e.printStackTrace(System.err);
                System.err.println("SQLState: " + ((SQLException) e).getSQLState());
                System.err.println("Error Code: " + ((SQLException) e).getErrorCode());
                System.err.println("Message: " + e.getMessage());
                Throwable t = ex.getCause();
                while (t != null) {
                    System.out.println("Cause: " + t);
                }
            }
        }
    }

    public List<User> selectUserByCountry(String countrySearch){
        List<User> users = new ArrayList<>();
        try(Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(SELECT_USER_BY_COUNTRY);){
            preparedStatement.setString(1, "%" + countrySearch + "%");
            getUser(users, preparedStatement);

        }
        catch (SQLException e){
            printSQLException(e);
        }
        return users;
    }

    private void getUser(List<User> users, PreparedStatement preparedStatement) throws SQLException {
        ResultSet rs = preparedStatement.executeQuery();
        while(rs.next()){
            int id = rs.getInt("id");
            String name = rs.getString("name");
            String email = rs.getString("email");
            String country = rs.getString("country");

            users.add(new User(id, name, email, country));
        }
    }

    public List<User> orderByName(String order){
        List<User> users = new ArrayList<>();
        String sql = order.equals("asc") ? ORDER_BY_NAME_ASC : ORDER_BY_NAME_DESC;
        try(Connection connection =  getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(sql);){
            System.out.println("prestatement: " + preparedStatement);
            getUser(users, preparedStatement);
        }catch (SQLException e){
            printSQLException(e);
        }
        return users;
    }

    public User getUserById(int id){
        User user = null;
        String query = "{Call get_user_by_id(?)}";

        try(Connection connection = getConnection(); CallableStatement callableStatement = connection.prepareCall(query);){
            callableStatement.setInt(1, id);
            ResultSet rs = callableStatement.executeQuery();

            while(rs.next()){
                String name = rs.getString("name");
                String email = rs.getString("email");
                String country = rs.getString("country");
                user = new User(id, name, email, country);
            }

        }catch (SQLException e){
            printSQLException(e);
        }
        return user;
    }

    public void insertUserStore(User user) throws SQLException{
        String query = "{Call insert_user(?,?,?)}";
        try(Connection connection = getConnection(); CallableStatement callableStatement = connection.prepareCall(query);){
            callableStatement.setString(1, user.getName());
            callableStatement.setString(2, user.getEmail());
            callableStatement.setString(3, user.getCountry());
            callableStatement.executeUpdate();
        }catch (SQLException e){
            printSQLException(e);
        }
    }

    public void addUserTransaction(User user, List<Integer> permissions) {
        Connection conn = null;
        // for insert a new user
        PreparedStatement pstmt = null;

        // for assign permision to user
        PreparedStatement pstmtAssignment = null;

        // for getting user id
        ResultSet rs = null;
        try {
            conn = getConnection();

            // set auto commit to false
            conn.setAutoCommit(false);

            // Insert user
            pstmt = conn.prepareStatement(INSERT_USER_SQL, Statement.RETURN_GENERATED_KEYS);
            pstmt.setString(1, user.getName());
            pstmt.setString(2, user.getEmail());
            pstmt.setString(3, user.getCountry());
            int rowAffected = pstmt.executeUpdate();

            // get user id
            rs = pstmt.getGeneratedKeys();

            int userId = 0;
            if (rs.next())
                userId = rs.getInt(1);

            // in case the insert operation successes, assign permision to user
            if (rowAffected == 1) {
                // assign permision to user
                String sqlPivot = "INSERT INTO user_permision(user_id,permision_id) "
                        + "VALUES(?,?)";
                pstmtAssignment = conn.prepareStatement(sqlPivot);

                for (int permisionId : permissions) {
                    pstmtAssignment.setInt(1, userId);
                    pstmtAssignment.setInt(2, permisionId);
                    pstmtAssignment.executeUpdate();
                }
                conn.commit();
            } else {
                conn.rollback();
            }

        } catch (SQLException ex) {
            // roll back the transaction
            try {
                if (conn != null)
                    conn.rollback();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            System.out.println(ex.getMessage());
        } finally {
            try {
                if (rs != null) rs.close();
                if (pstmt != null) pstmt.close();
                if (pstmtAssignment != null) pstmtAssignment.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Override
    public void insertUpdateWithoutTransaction() {
        try (Connection conn = getConnection();
             Statement statement = conn.createStatement();
             PreparedStatement psInsert = conn.prepareStatement(SQL_INSERT);
             PreparedStatement psUpdate = conn.prepareStatement(SQL_UPDATE)) {
            statement.execute(SQL_TABLE_DROP);
            statement.execute(SQL_TABLE_CREATE);

            // Run list of insert commands
            psInsert.setString(1, "Quynh");
            psInsert.setBigDecimal(2, new BigDecimal(10));
            psInsert.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
            psInsert.execute();

            psInsert.setString(1, "Ngan");
            psInsert.setBigDecimal(2, new BigDecimal(20));
            psInsert.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
            psInsert.execute();

            // Run list of update commands
            // below line caused error, test transaction
            // org.postgresql.util.PSQLException: No value specified for parameter 1.
            psUpdate.setBigDecimal(2, new BigDecimal(999.99));
            //psUpdate.setBigDecimal(1, new BigDecimal(999.99));
            psUpdate.setString(2, "Quynh");
            psUpdate.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
