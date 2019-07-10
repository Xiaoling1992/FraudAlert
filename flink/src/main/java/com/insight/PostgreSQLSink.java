package com.insight;

//import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.apache.flink.types.Row;


public class PostgreSQLSink extends RichSinkFunction< String[] > {

    private static final long serialVersionUID = 1L;

    private Connection connection;
    private PreparedStatement preparedStatement;
    /**
     * open will be executed once before invoke method
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        String USERNAME = "db_select" ;
        String PASSWORD = "password";
        String DRIVERNAME = "org.postgresql.Driver";
        String DBURL = "jdbc:postgresql://10.0.0.4:5431/test_db";
        Class.forName(DRIVERNAME);
        // get the access to the database.
        connection = DriverManager.getConnection(DBURL,USERNAME,PASSWORD);
        String sql = "insert into transactions(key,index,phonenumber,time,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v17,v18,v19,v20,v21,v22,v23,v24,v25,v26,v27,v28,amount,timeproduced,timeprocessed,latency,prediction,reply) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        super.open(parameters);
    }

    /**
     * invoke() analyzed one element and insert it to the databbase
     * @param row 
     * @throws Exception
     */
    //@Override
    public  void invoke(String[] row) throws Exception{
        try {
            /*for(int i=0; i<=37; ++i) {
                if (i <= 1) {
                    preparedStatement.setInt(i + 1, (int)row.getField(i) );
                } else if (i == 2) {
                    preparedStatement.setString(i + 1, row.getField(i).toString() );
                } else if (i <= 35) {
                    preparedStatement.setDouble(i + 1, (double) row.getField(i) );
                } else {
                    preparedStatement.setInt(i + 1, (int) row.getField(i) );
                }
            }*/

            for(int i=0; i<=37; ++i) {
                preparedStatement.setString(i + 1, row[i] );
            }

            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }

    };

    /**
     * close()是tear down的方法，在销毁时执行，关闭连接。
     */
    @Override
    public void close() throws Exception {
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }
}
