package org.sid;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Properties;

public class HopitalProcessor {

    // --- Configuration de la Connexion MySQL ---
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/DB_HOPITAL";
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    // *** À MODIFIER *** : Remplacer par vos identifiants MySQL
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) {

        // 1. Initialisation de la SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("HopitalTraitementDonnees")
                // Utilisation de local[*] pour les tests en mode autonome
                .config("spark.master", "local[*]")
                .getOrCreate();

        // 2. Définition des propriétés de connexion JDBC
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", DB_USER);
        connectionProperties.put("password", DB_PASSWORD);
        connectionProperties.put("driver", JDBC_DRIVER);

        System.out.println("Début du traitement des données MySQL...");

        try {
            // 3. Chargement des tables dans des DataFrames et création de vues temporaires
            loadTable(spark, "MEDECINS", connectionProperties).createOrReplaceTempView("MEDECINS");
            loadTable(spark, "CONSULTATIONS", connectionProperties).createOrReplaceTempView("CONSULTATIONS");

            // 4. Exécution des Requêtes Spark SQL
            executeQueries(spark);

        } catch (Exception e) {
            System.err.println("Échec de la connexion ou du traitement des données: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 5. Arrêt de la session Spark
            spark.stop();
        }
    }

    /**
     * Charge une table MySQL spécifique dans un DataFrame Spark.
     */
    private static Dataset<Row> loadTable(SparkSession spark, String tableName, Properties prop) {
        System.out.println("  -> Chargement de la table: " + tableName);
        return spark.read().jdbc(JDBC_URL, tableName, prop);
    }

    /**
     * Exécute et affiche les résultats des requêtes demandées.
     */
    private static void executeQueries(SparkSession spark) {

        // --- REQUÊTE 1 : Afficher le nombre de consultations par jour ---
        System.out.println("\n*** Résultat 1: Nombre de consultations par jour ***");
        spark.sql(
                "SELECT " +
                        "DATE_CONSULTATION, " +
                        "COUNT(ID) AS NOMBRE_CONSULTATIONS " +
                        "FROM CONSULTATIONS " +
                        "GROUP BY DATE_CONSULTATION " +
                        "ORDER BY DATE_CONSULTATION"
        ).show();

        // --- REQUÊTE 2 : Afficher le nombre de consultation par médecin (NOM | PRENOM | COUNT) ---
        System.out.println("\n*** Résultat 2: Nombre de consultations par médecin ***");
        spark.sql(
                "SELECT " +
                        "M.NOM, " +
                        "M.PRENOM, " +
                        "COUNT(C.ID) AS NOMBRE_DE_CONSULTATION " +
                        "FROM MEDECINS M " +
                        "LEFT JOIN CONSULTATIONS C ON M.ID = C.ID_MEDECIN " +
                        "GROUP BY M.ID, M.NOM, M.PRENOM " +
                        "ORDER BY NOMBRE_DE_CONSULTATION DESC"
        ).show();

        // --- REQUÊTE 3 : Afficher pour chaque médecin, le nombre de patients qu’il a assisté ---
        System.out.println("\n*** Résultat 3: Nombre de patients uniques assistés par médecin ***");
        spark.sql(
                "SELECT " +
                        "M.NOM, " +
                        "M.PRENOM, " +
                        "COUNT(DISTINCT C.ID_PATIENT) AS NOMBRE_DE_PATIENTS_ASSISTES " +
                        "FROM MEDECINS M " +
                        "LEFT JOIN CONSULTATIONS C ON M.ID = C.ID_MEDECIN " +
                        "GROUP BY M.ID, M.NOM, M.PRENOM " +
                        "ORDER BY NOMBRE_DE_PATIENTS_ASSISTES DESC"
        ).show();
    }
}