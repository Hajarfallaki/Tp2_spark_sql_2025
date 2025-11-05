# TP 2 : Spark SQL

## 🧠 Introduction
Ce projet a pour objectif de manipuler et d’analyser des données à l’aide de **Spark SQL**, un module essentiel d’**Apache Spark** permettant le traitement distribué de données structurées.  
À travers deux exercices, nous avons exploré la puissance de Spark pour exécuter des requêtes SQL sur des **DataFrames** et des **Datasets**, et interagir avec différentes sources de données (fichiers CSV et base MySQL).

---

## ⚙️ Environnement et technologies
- **Langage :** Java  
- **Framework :** Apache Spark  
- **Version :** Spark 3.x  
- **Base de données :** MySQL  
- **IDE recommandé :** IntelliJ IDEA / VS Code  
- **Build tool :** Maven  

---

## 🧩 Exercice 1 — Analyse des incidents par service
Une entreprise industrielle souhaite analyser les incidents survenus dans ses services.  
Les données sont stockées dans un fichier CSV au format suivant :  
`Id, titre, description, service, date`

### 🎯 Objectifs :
1. Afficher le **nombre d’incidents par service**.  
2. Afficher les **deux années ayant le plus d’incidents**.

### 💡 Fichiers concernés :
- `src/main/java/org/sid/AnalyseIncidents.java`  
- `src/main/resources/incidents.csv`

---

## 🏥 Exercice 2 — Traitement des données hospitalières (MySQL + Spark SQL)
L’hôpital national souhaite exploiter ses données médicales à l’aide de Spark SQL pour effectuer des analyses distribuées.  
Les données sont stockées dans une base **MySQL** nommée `DB_HOPITAL` contenant trois tables :
- `PATIENTS`
- `MEDECINS`
- `CONSULTATIONS`

### 🎯 Objectifs :
- Afficher le **nombre de consultations par jour**.  
- Afficher le **nombre de consultations par médecin** sous le format :  
  `NOM | PRENOM | NOMBRE_DE_CONSULTATIONS`  
- Afficher, pour chaque médecin, le **nombre de patients distincts**.

### 💡 Fichiers concernés :
- `src/main/java/org/sid/HopitalProcessor.java`

---

J'ai bien compris. Vous souhaitez que je sépare les deux sections de votre texte de signature et d'exécution.

Voici le texte séparé et formaté :


### 👩‍💻 RÉALISÉ PAR

Hajar Elfallaki-Idrissi
Étudiante ingénieure en Data & Intelligence Artificielle & Cloud Computing
ENSET Mohammedia — 2025
