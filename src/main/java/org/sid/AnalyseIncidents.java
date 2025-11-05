package org.sid;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class AnalyseIncidents {

    public static void main(String[] args) {

        // Initialiser la session Spark
        SparkSession spark = SparkSession.builder()
                .appName("Analyse des Incidents")
                .master("local[*]")
                .getOrCreate();

        // Charger les données depuis le dossier resources
        // Méthode 1 : Via ClassLoader (recommandé pour les resources)
        String csvPath = AnalyseIncidents.class.getClassLoader()
                .getResource("incidents.csv").getPath();

        Dataset<Row> dfIncidents = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csvPath);

        // Afficher un aperçu des données
        System.out.println("=== Aperçu des données ===");
        dfIncidents.show(5);
        System.out.println("\nSchéma des données:");
        dfIncidents.printSchema();

        // ============================================
        // Question 1: Nombre d'incidents par service
        // ============================================
        System.out.println("\n=== Question 1: Nombre d'incidents par service ===");

        Dataset<Row> incidentsParService = dfIncidents
                .groupBy("service")
                .agg(count("Id").alias("nombre_incidents"))
                .orderBy(desc("nombre_incidents"));

        incidentsParService.show();

        // Optionnel: Sauvegarder les résultats
        // incidentsParService.write()
        //         .option("header", "true")
        //         .mode("overwrite")
        //         .csv("resultats/incidents_par_service");

        // =====================================================
        // Question 2: Les deux années avec le plus d'incidents
        // =====================================================
        System.out.println("\n=== Question 2: Les deux années avec le plus d'incidents ===");

        Dataset<Row> incidentsParAnnee = dfIncidents
                .withColumn("annee", year(col("date")))
                .groupBy("annee")
                .agg(count("Id").alias("nombre_incidents"))
                .orderBy(desc("nombre_incidents"))
                .limit(2);

        incidentsParAnnee.show();

        // Optionnel: Sauvegarder les résultats
        // incidentsParAnnee.write()
        //         .option("header", "true")
        //         .mode("overwrite")
        //         .csv("resultats/top2_annees");

        // Afficher les statistiques globales
        System.out.println("\n=== Statistiques globales ===");
        System.out.println("Nombre total d'incidents: " + dfIncidents.count());
        System.out.println("Nombre de services distincts: " +
                dfIncidents.select("service").distinct().count());

        // Fermer la session Spark
        spark.stop();
    }
}