JOUR 2  Silver Layer + Scraping Hotellerie
Nettoyer la donnee KBO et lancer le scraping financier sur le secteur hotelier

Ou on en est
Fin du Jour 1, tout est en place : la collection enterprise_finale existe dans MongoDB, les scripts de scraping NBB, STAPOR et eJustice sont operationnels, et la StateDB (collection MongoDB) est initialisee avec les entreprises cibles en status=pending.

Jour 2, on s'attaque a deux choses : nettoyer et enrichir enterprise_finale pour en faire une couche Silver exploitable, puis lancer le scraping financier NBB cible sur le secteur hotelier depuis 2021.

  Resultat attendu en fin de journee
  Silver applique sur enterprise_finale  dates normalisees, activites dedupliquees, labels decodes, adresse unique
  Filtre hotellerie operationnel  liste des entreprises cibles extraite et chargee en StateDB
  Scraping NBB lance  depots financiers 2021 a 2025 telecharges pour chaque entreprise hoteliere
  StateDB a jour  status=done pour chaque depot scrape avec succes



PART 1  Nettoyage Silver (MongoDB)
On ne modifie pas enterprise_finale  elle reste intacte comme couche Bronze. On cree une nouvelle collection enterprise_silver qui contient les documents transformes. 

1. Normalisation des dates
StartDate est stocke en DD-MM-YYYY (string). On le convertit en YYYY-MM-DD pour pouvoir faire des vraies comparaisons de dates dans les requetes MongoDB.

Avant	Apres
"02-01-2021"	"2021-01-02"
"16-03-1880"	"1880-03-16"

2. Deduplication des activites
Chaque entreprise a ses activites listees plusieurs fois : une fois par version NACE (2003, 2008, 2025). On deduplique uniquement si le code est exactement identique  70220 (Nace2008) et 70200 (Nace2025) sont deux codes differents et doivent etre conserves tous les deux.



Regle	Exemple
Meme NaceCode exact + meme Classification -> dedupliquer	62020 MAIN (2008) x2 -> garder 1 seul
Codes differents -> garder les deux	70220 (2008) et 70200 (2025) -> conserver les deux
MAIN et SECO conserves	Ne pas supprimer les activites secondaires, juste les vrais doublons

3. Adresse unique
On garde uniquement TypeOfAddress = REGO (siege social enregistre). Les autres adresses sont supprimees du document Silver.

4. Denomination principale
TypeOfDenomination = 1 = nom officiel. On le garde en premier, les autres restent en secondaire.

5. Decodage des codes -> labels
On ajoute les labels FR a cote des codes bruts via code.csv :

Champ code	Champ label ajoute
JuridicalForm: 610	JuridicalFormLabel: "Societe a responsabilite limitee"
Status: "AC"	StatusLabel: "Actif"
activities[].NaceCode	activities[].NaceLabel (description FR Nace2008 + Nace2025)

On conserve les codes originaux  utiles pour filtrer et indexer. On ajoute uniquement les labels pour l'affichage.



PART 2  Ciblage hotellerie + Scraping NBB
Le scraping financier NBB (CBSO) est cible uniquement sur le secteur hotelier. On filtre enterprise_finale pour extraire les entreprises concernees, on les charge en StateDB, puis on scrape leurs depots depuis 2021.

Codes NACE hotellerie retenus
Code	Description
55100	Hotels et hebergement similaire
55201	Auberges de jeunesse
55202	Centres et villages de vacances
55203	Gites de vacances, appartements et meubles de vacances
55204	Chambres d'hotes
55209	Autres hebergements de courte duree n.c.a.
55300	Terrains de camping et parcs pour caravanes
55400	Intermediation pour l'hebergement  Nace2025 (type Airbnb/Booking)
55900	Autres hebergements

Filtres appliques
Critere	Valeur
Status	AC (actif uniquement)
TypeOfEnterprise	2 (personne morale privee)
Classification activite	MAIN uniquement
NaceCode	Dans la liste des 9 codes hotellerie ci-dessus
JuridicalForm exclus  entites publiques	110, 114, 116, 117
JuridicalForm exclus  services federaux	301, 302, 303
JuridicalForm exclus  autorites regionales	310, 320, 330, 340, 350
JuridicalForm exclus  communes, CPAS, intercommunales	400, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420
Scraping NBB CBSO  depots depuis 2021
Pour chaque entreprise hoteliere identifiee, on recupere les depots financiers des exercices 2021, 2022, 2023, 2024 et 2025 via l'API NBB CBSO.

Etape	Description
Lister les depots	GET /api/enterprises/{bce}/filings  liste tous les exercices disponibles
Filtrer >= 2021	Garder uniquement les depots dont accountingYearEndDate >= 2021-01-01
Telecharger le CSV	GET /api/filings/{reference}/document  CSV avec codes PCMN et valeurs
Stocker sur HDFS	HDFS /<id>/<nbb>/<year>/<ref>
Mettre a jour StateDB	status=done, filings_count=N apres scrape complet de l'entreprise

Gestion des 429 (rate limit NBB)
Le NBB peut retourner un 429 Too Many Requests si on scrape trop vite. Dans ce cas, seule une partie des entreprises sera scrapee avant le blocage. La StateDB permet de reprendre proprement sans tout relancer.
N’oubliez pas de tracker les fichiers que vous avez déjà télécharger a travers votre stateDB




Statut StateDB	Signification
pending	Pas encore scrape  a traiter
in_progress	Scraping en cours  ne pas retoucher
done	Scrape avec succes  depots en Bronze

use this if needed /workspaces/Data-Platform/TP2/silver/strapor.py
/workspaces/Data-Platform/TP2/silver/consult.py