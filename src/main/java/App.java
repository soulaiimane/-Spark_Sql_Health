import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

public class App {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().appName("Tp Spark Sql").master("local[*]").getOrCreate();
        HashMap<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_HOPITAL");
        options.put("user","root");
        options.put("password","");

        //1- Show the number of consultations per day.
        Dataset<Row> df1 = ss.read().format("jdbc")
                .options(options)
                .option("query", " select DATE_CONSULTATION , COUNT(ID) as nbConsultations from CONSULTATIONS GROUP BY DATE_CONSULTATION")
                .load();
                df1.show();

        //2- Display the number of consultations per doctor.
        Dataset<Row> df2= ss.read().format("jdbc")
                .options(options)
                .option("query", " select MEDECINS.NOM, MEDECINS.PRENOM , COUNT(CONSULTATIONS.ID) AS nbConsultations from MEDECINS " +
                        "JOIN CONSULTATIONS " +
                        "ON CONSULTATIONS.ID_MEDECIN=MEDECINS.ID "+
                        "GROUP BY MEDECINS.NOM, MEDECINS.PRENOM ,MEDECINS.ID")
                .load();
                df2.show();
        //3- Display for each doctor the number of patients he has attended.
        Dataset<Row> df3= ss.read().format("jdbc")
                .options(options)
                .option("query", " select MEDECINS.NOM, MEDECINS.PRENOM , COUNT(DISTINCT CONSULTATIONS.ID_PATIENT) AS nbPatients from MEDECINS " +
                        "JOIN CONSULTATIONS " +
                        "ON CONSULTATIONS.ID_MEDECIN=MEDECINS.ID "+
                        "GROUP BY MEDECINS.NOM, MEDECINS.PRENOM ,MEDECINS.ID")
                .load();
        df3.show();

    }
}
