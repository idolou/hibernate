import oracle.jdbc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class Assignment {
    private String ConnectionURL;
    private String DB_username;
    private String DB_pswd;
    private String driver_conn=null;


    public Assignment(String connection, String username, String password) {
        ConnectionURL = connection;
        DB_username = username;
        DB_pswd = password;
        driver_conn = "oracle.jdbc.driver.OracleDriver";


    }

    public void closeconn(Connection conn){
        try{
            if (conn != null){
                conn.close();
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }
    }


    public void fileToDataBase(String CVSpath) {
        Connection conn = null;
        PreparedStatement prst = null;
        BufferedReader buffr = null;
        String Line = "";
        String ins_query = "INSERT INTO MEDIAITEMS(TITLE,PROD_YEAR)"+
                " VALUES(?,?)";
        List<String> TITLE = new ArrayList<String>();
        List<String> PROD_YEAR = new ArrayList<String>();


            try {
                buffr = new BufferedReader(new FileReader(CVSpath));
                while ((Line = buffr.readLine()) != null) {
                    String[] mediaItems = Line.split(",");
                    TITLE.add(mediaItems[0]);
                    PROD_YEAR.add(mediaItems[1]);
                }
            } catch(IOException exc){
            exc.printStackTrace();
        } finally {
                if (buffr != null) {
                    try {buffr.close();
                    } catch (IOException exc) {
                        exc.printStackTrace();}
                }
            }
        try{
            Class.forName(driver_conn);
            conn = DriverManager.getConnection(ConnectionURL, DB_username, DB_pswd);
            prst = conn.prepareStatement(ins_query);
            int i = 0;
            while(i < PROD_YEAR.size()){
                prst.setString(1, TITLE.get(i));
                prst.setInt(2, Integer.parseInt(PROD_YEAR.get(i)));
                prst.executeUpdate();
                conn.commit();
                i++;
            }
            prst.close(); }
       catch (SQLException | ClassNotFoundException Sexc){
            Sexc.printStackTrace();
       } finally{try{
            if (prst != null){prst.close();}}
        catch (SQLException e){
            e.printStackTrace();
        }
        closeconn(conn);
        }
        }






//    calculateSimilarity//////////////////////////////////////////
    public void calculateSimilarity(){
        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement prst = null;
        String ins_query = "";
        long max_dist = 0;
        float sim = 0;
        List<Long> Similarities = new ArrayList<Long>();
        try{
            Class.forName(driver_conn);
            conn = DriverManager.getConnection(ConnectionURL, DB_username, DB_pswd);
            String max_query = "";

            CallableStatement cstmt = conn.prepareCall("{? = call MaximalDistance()}");
            cstmt.registerOutParameter(1, oracle.jdbc.OracleTypes.NUMBER);
            cstmt.execute();
            max_dist = cstmt.getLong(1);
            String get_mid = "SELECT MID FROM MEDIAITEMS";
            prst = conn.prepareStatement(get_mid);
            rs = prst.executeQuery();
            while (rs.next())
            {Similarities.add(rs.getLong(1));}
            cstmt.close();
            rs.close();

            int i = 0;
            while (i < Similarities.size()-1){
                for (int j = i+1 ; j < Similarities.size(); j++){
                    CallableStatement cstmt_1 = conn.prepareCall("{? = call SimCalculation(?, ?, ?)}");
                    cstmt_1.registerOutParameter(1, OracleTypes.FLOAT);
                    cstmt_1.setLong(2, Similarities.get(i));
                    cstmt_1.setLong(3, Similarities.get(j));
                    cstmt_1.setLong(4, max_dist);
                    cstmt_1.execute();
                    sim = cstmt_1.getFloat(1);
                    cstmt_1.execute();
                    cstmt_1.close();
                    Insert_similarity(Similarities.get(i), Similarities.get(j), sim);
                }
                i++;
                }

            }
        catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (prst != null) {
                    prst.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

    }

    public void Insert_similarity(long mid1, long mid2, float similarity) {
        Connection conn = null;
        PreparedStatement prst = null;
        String insert_query = "INSERT INTO Similarity(MID1,MID2, SIMILARITY)VALUES(?,?,?)";

        try{
            Class.forName(driver_conn);
            conn = DriverManager.getConnection(ConnectionURL, DB_username, DB_pswd);
            prst = conn.prepareStatement(insert_query);
            prst.setLong(1,mid1);
            prst.setLong(2,mid2);
            prst.setDouble(3,similarity);
            prst.executeUpdate();
            conn.commit();
            prst.close();
        }
        catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (prst != null) {
                    prst.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }





    public static void main(String[] args) {

        String username = "idolou";
        String password = "abcd";
        String connection = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521:oracle";
        String path = "";




    }


}
