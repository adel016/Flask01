from flask import Flask, render_template, request, url_for, redirect
import requests
from datetime import datetime
import pandas as pd
import sqlite3
import os
import csv
import matplotlib.pyplot as plt


# Déclaration d'application Flask
app = Flask(__name__)

# Configuration pour servir les fichiers statiques
app.static_folder = 'static'

# Nom de la base de données SQLite
db = "database.sqlite3"

# Chemin relatif vers la base de données
db_path = os.path.join(os.path.dirname(__file__), 'data', db)

url_base = "https://files.data.gouv.fr/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/"

# Fonction pour se connecter à la base de données
def connect_db():
    # Connection à la base
    return sqlite3.connect(db_path)


# Fonction pour obtenir la liste des catégories depuis la base de données
def Organismes():
    conn = connect_db()

    # Obtention des catégories
    query = "SELECT * FROM Organisme"
    # Utilisation de pandas pour exécuter la requête SQL
    organismes = pd.read_sql_query(query, conn)

    # Fermeture de la connexion à la base de données
    conn.close()
    return organismes

def Zas():
    conn = connect_db()
    query = "SELECT * FROM Zas"
    zas = pd.read_sql_query(query, conn)
    conn.close()
    return zas

def Site():
    conn = connect_db()
    query = "SELECT * FROM Site"
    site = pd.read_sql_query(query, conn)
    conn.close()
    return site

def Mesure():
    conn = connect_db()
    query = "SELECT * FROM Mesure"
    mesure = pd.read_sql_query(query, conn)
    conn.close()
    return mesure

def Polluant():
    conn = connect_db()
    query = "SELECT * FROM Polluant"
    polluant = pd.read_sql_query(query, conn)
    conn.close()
    return polluant

def Delet():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM Mesure;')
    cursor.execute('DELETE FROM Organisme;')
    cursor.execute('DELETE FROM Polluant;')
    cursor.execute('DELETE FROM Site;')
    cursor.execute('DELETE FROM Zas;') 
    conn.commit()
    conn.close()

def insert():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    with open('doc2.csv', 'r') as csv_file:
        # La première ligne est passée (headers)
        next(csv_file)
        
        # Lecture du fichier CSV et insertion des données dans les tables
        csv_data = csv.reader(csv_file, delimiter=";")
        for row in csv_data:
            # Insertion ou récupération de l'id de l'organisme
            cursor.execute('INSERT OR IGNORE INTO Organisme (nom_orga) VALUES (?)', (row[2],))
            cursor.execute('SELECT Id_Orga FROM Organisme WHERE nom_orga = ?', (row[2],))
            Id_Orga = cursor.fetchone()[0]

            # Insertion ou récupération de l'id du Zas
            cursor.execute('INSERT OR IGNORE INTO Zas ( Code_zas, Nom_zas, Id_Orga) VALUES (?, ?, ?)', (row[3],row[4],Id_Orga))
            cursor.execute('SELECT Id_Zas FROM Zas WHERE code_zas = ?', (row[3],))
            Id_Zas = cursor.fetchone()[0]

            # Insertion ou récupération du l'id du Site
            cursor.execute('INSERT OR IGNORE INTO Site (Code_site, Nom_Site, Type_Impl, Id_Zas) VALUES (?, ?, ?, ?)', (row[5],row[6], row[7],Id_Zas))
            cursor.execute('SELECT Id_Site FROM SITE WHERE Nom_Site = ?', (row[6],))
            Id_Site = cursor.fetchone()[0]
           # Insertion ou Récupération de l'id de Pollution
            cursor.execute('INSERT OR IGNORE INTO Polluant (nom_polluant, Type_Influ) VALUES (?, ?)', (row[8], row[9])) 
            cursor.execute('SELECT Id_Polluant FROM Polluant WHERE nom_polluant = ?', (row[8],))
            Id_Polluant = cursor.fetchone()[0]
            
            # Insertion ou récupération de l'id de la Mesure
            cursor.execute('INSERT OR IGNORE INTO Mesure ( Id_Site, Id_Polluant, Date_Deb, Date_fin, Discriminant, Type_Valeur, Valeur_Brute, Unit, Taux_Saisi, Couverture_Temp, Validite) VALUES (?,?,?,?,?,?,?,?,?,?,?)', (Id_Site, Id_Polluant , row[0],row[1],row[10],row[14],row[16],row[17],row[18], row[19],row[22]))

    conn.commit()
    conn.close()
            


@app.route('/', methods=['GET', 'POST'])
def index():

    if request.method == 'POST':
        annee = int(request.form['annee'])
        mois = int(request.form['mois'])
        jour = int(request.form['jour'])

    
        # Validate date
        try:
            date = datetime(annee, mois, jour)
        except ValueError as e:
            print("Invalid date:", e)
            return render_template('index.html', error="Invalid date")

        file_name = f"FR_E2_{date.strftime('%Y-%m-%d')}.csv"
        url_base = f"https://files.data.gouv.fr/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/{annee}/"
        url_file = url_base + file_name

        response = requests.get(url_file)
        if response.status_code == 200:
            open("doc2.csv","wb").write(response.content)#creation dossier avec valeur
            Delet()
            insert()
            return redirect(url_for('organismes_page'))
        else:
            print("Failed to download the file.")

    organismes = Organismes().to_dict(orient='records')

    return render_template('index.html', organismes=organismes)

@app.route('/comparaison')
def comparaison():
    # Add any necessary data or logic you might want to pass to the template
    return render_template('comparaison.html')

@app.route('/about')
def about():
    # Add any necessary data or logic you might want to pass to the template
    return render_template('about.html')


@app.route('/actus')
def actus():
    # Add any necessary data or logic you might want to pass to the template
    return render_template('actus.html')

@app.route('/organismes')
def organismes_page():
    organismes = Organismes().to_dict(orient='records')
    mesures = Mesure().to_dict(orient='records')
    return render_template('organisme.html', organismes=organismes, mesures=mesures)


@app.route('/organisme/zas/<int:id_orga>')
def handle_organisme(id_orga):
    conn = connect_db()

    # Récupération des informations de l'organisme spécifique
    query_organisme = f"SELECT * FROM Organisme WHERE Id_Orga = {id_orga}"
    organisme_info = pd.read_sql_query(query_organisme, conn)

    # Récupération des zas associées à cet organisme
    query_zas = f"SELECT * FROM Zas WHERE Id_Orga = {id_orga}"
    zas_info = pd.read_sql_query(query_zas, conn)

    #conn.close()

        # Préparation des données pour le template
    if not organisme_info.empty:
        organisme_info = organisme_info.to_dict(orient='records')[0]
    else:
        organisme_info = {}

    zas = zas_info.to_dict(orient='records') if not zas_info.empty else []

    # Affichage des données des ZAs
    zas = zas_info.to_dict(orient='records') if not zas_info.empty else []

# Itérer sur les valeurs du dictionnaire zas
    for item in zas:
        print(f"ID Zas: {item['Id_Zas']}, Nom Zas: {item['Nom_zas']}")

    return render_template('zas.html', organisme=organisme_info, zas=zas)

@app.route('/zas/<int:id_zas>')
def handle_zas(id_zas):
    conn = connect_db()

    # Récupération des informations du Zas spécifique
    query_zas = f"SELECT * FROM Zas WHERE Id_Zas = {id_zas}"
    zas_info = pd.read_sql_query(query_zas, conn)

    # Récupération des sites associés à ce Zas
    query_sites = f"SELECT * FROM Site WHERE Id_Zas = {id_zas}"
    sites_info = pd.read_sql_query(query_sites, conn)

    conn.close()

    zas = zas_info.to_dict(orient='records')[0] if not zas_info.empty else {}
    sites = sites_info.to_dict(orient='records') if not sites_info.empty else []

    return render_template('site.html', zas=zas, sites=sites)

@app.route('/site/mesures/<int:id_site>')
def handle_site_mesures(id_site):
    conn = connect_db()

    # Nous devons également joindre la table Polluant pour obtenir le nom du polluant
    query_mesures = f"""
    SELECT m.Date_Deb, p.nom_polluant, m.valeur_brute, m.Unit
    FROM Mesure m
    JOIN Polluant p ON m.ID_Polluant = p.Id_Polluant
    WHERE m.Id_Site = {id_site}
    """
    cursor = conn.cursor()
    cursor.execute("UPDATE Mesure SET Unit = REPLACE(Unit, 'Â', '') WHERE Unit LIKE 'Â%';")
    mesures_info = pd.read_sql_query(query_mesures, conn)
    conn.close()

    mesures = mesures_info.to_dict(orient='records') if not mesures_info.empty else []
    
    # Pour le débogage, affichez les mesures récupérées
    print(mesures)

    return render_template('test.html', mesures=mesures)

@app.route('/site/mesures/<int:id_site>/graphe')
def graph(id_site):

    conn = connect_db()
    cursor = conn.cursor()

    # Mise à jour des données (si nécessaire)
    cursor.execute("UPDATE Mesure SET Unit = REPLACE(Unit, 'Â', '') WHERE Unit LIKE 'Â%';")

    # Récupération des mesures détaillées
    query_mesures = f"""
    SELECT m.Date_Deb, p.nom_polluant, m.valeur_brute, m.Unit
    FROM Mesure m
    JOIN Polluant p ON m.ID_Polluant = p.Id_Polluant
    WHERE m.Id_Site = {id_site}
    """
    mesures_info = pd.read_sql_query(query_mesures, conn)
    mesures = mesures_info.to_dict(orient='records') if not mesures_info.empty else []

    # Calcul des moyennes pour chaque polluant
    polluants = ['NO', 'NO2', 'O3', 'NOx', 'PM10', 'PM2.5', 'C6H6', 'SO2', 'CO']
    moyennes = {}
    for polluant in polluants:
        query = f"""
        SELECT AVG(valeur_brute) as moyenne
        FROM Mesure m
        JOIN Polluant p ON m.ID_Polluant = p.Id_Polluant
        WHERE m.Id_Site = {id_site} AND p.nom_polluant = '{polluant}'
        """
        moyenne = pd.read_sql_query(query, conn)
        moyenne_valeur = moyenne['moyenne'].iloc[0]
        moyennes[polluant] = 0 if moyenne_valeur is None else moyenne_valeur

    conn.close()

    # Génération des graphiques
    generate_graphs(moyennes)
    generate(id_site)

    return render_template('graph.html', mesures=mesures, moyennes=moyennes)

def generate_graphs(moyennes):
    polluants = list(moyennes.keys())
    valeurs = list(moyennes.values())

    plt.bar(polluants, valeurs)
    plt.xlabel('Polluants')
    plt.ylabel('Taux')
    plt.title('Evolution des Taux de Polluants dans la journée')
    for i, valeur in enumerate(valeurs):
        plt.text(i, valeur + 0.5, str(valeur), ha='center', va='bottom')
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'static', 'graphique_polluants.png')
    plt.savefig(path)
    plt.clf()

def generate(id_site):
    db = 'database.sqlite3'
    db_path = os.path.join(os.path.dirname(__file__), 'data', db)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()


# Liste des polluants disponibles
    polluants_disponibles = ["NO", "NO2", "O3", "NOx", "PM10", "PM2", "C6H6", "SO2", "CO"]

    # Liste pour stocker les données des polluants
    donnees_polluants = []

    # Parcours des tables pour chaque polluant disponible
    for polluant in polluants_disponibles:
        # Construire la requête SQL en fonction du nom du polluant
        id_polluant = polluants_disponibles.index(polluant) + 1
        query = f"SELECT ROUND(avg(CASE WHEN Unit LIKE '%µg%' AND polluant.Id_polluant = '{id_polluant}' THEN Mesure.valeur_brute WHEN Unit LIKE '%mg%' AND polluant.Id_polluant = '{id_polluant}' THEN Mesure.valeur_brute * 1000 ELSE 0 END), 2) as moyenne, Date_Deb FROM mesure JOIN polluant ON polluant.Id_polluant = mesure.Id_polluant where id_site = {id_site} GROUP BY Date_Deb"
        
        # Exécution de la requête SQL
        cursor.execute(query)
        
        # Récupération des résultats de la requête
        moyennes = cursor.fetchall()
        
        # Stockage des moyennes dans la liste appropriée
        donnees_polluants.append({"Nom": polluant, "Moyennes": [table[0] for table in moyennes]})

    # Génération du graphique en utilisant la fonction
    for polluant_data in donnees_polluants:
        nom_polluant = polluant_data["Nom"]
        moyennes = polluant_data["Moyennes"]

        plt.plot(range(24), moyennes, label=nom_polluant)
        plt.scatter(range(24), moyennes)

    plt.xlabel('Heure')
    plt.ylabel('Moyenne')
    plt.title('Moyenne des Valeurs des Polluants')
    plt.legend()
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'static', 'graphique.png')
    plt.savefig(path)
    plt.clf() 

if __name__ == "__main__":
    app.run(debug=True)
