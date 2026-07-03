JOUR 3  Gold Layer + API + Frontend
Calculer les ratios financiers, exposer une API FastAPI et afficher les resultats en React

Ou on en est
Fin du Jour 2 : les depots financiers NBB (CSVs PCMN) sont stockes dans HDFS sous {bce}/hbb/. La collection enterprise_silver contient les entreprises nettoyees. StateDB indique status=done pour chaque entreprise dont tous les exercices sont telecharges.

Jour 3 : on lit ces CSVs bruts depuis HDFS, on les parse pour extraire les ratios financiers, on consolide tout en une couche Gold dans MongoDB, puis on expose les donnees via une API FastAPI et un frontend React.

  Resultat attendu en fin de journee
  Gold layer peuplee  hotel_gold contient un document par entreprise avec tous les exercices calcules
  API FastAPI operationnelle  les donnees Gold sont interrogeables par le frontend
  Frontend React  affichage des fiches entreprise avec Sankey et tableaux de ratios
  Streaming SSE  les documents statuts notaire se chargent en temps reel



Part 1  Gold Layer (Spark + MongoDB)
On lit les CSVs PCMN bruts depuis HDFS ({bce}/hbb/) avec Spark, on calcule les ratios financiers pour chaque exercice, et on consolide tout en un seul document par entreprise dans MongoDB.

Structure HDFS
Chaque entreprise a un sous-repertoire {bce}/hbb/ contenant un CSV par exercice depose. Les fichiers sont lus en parallele par Spark.

Chemin	Contenu
{bce}/hbb/filing_ref.csv	CSV brut NBB avec codes PCMN et montants
Format CSV	code_pcmn | valeur (deux colonnes, separateur ;)

Parsing PCMN
Chaque ligne du CSV correspond a un code PCMN et un montant. On extrait les codes qui nous interessent et on les mappe sur des champs metier :

Code PCMN	Champ Gold
70	chiffre_affaires
60	achats
71	variation_stocks
9901	ebit
9904	resultat_net
54 + 55	tresorerie
17 + 43	dettes_financieres
10 a 15	fonds_propres
100	capital_souscrit

Ratios calcules par exercice
Ratio	Formule
Marge brute	CA - Achats + Variation stocks
Marge nette (%)	Resultat net / CA * 100
ROE (%)	Resultat net / Fonds propres * 100
Ratio de liquidite	Tresorerie / Dettes financieres
Taux endettement (%)	Dettes financieres / Fonds propres * 100

Schema hotel_gold (un document par entreprise)
Un seul document par entreprise, contenant tous les exercices sous forme de tableau. Cela evite les jointures multi-documents et permet de recuperer l'historique complet en une seule requete.

Champ	Contenu
enterprise_number	Numero BCE (cle unique)
years	Tableau d'objets : [{year, ca, marge_brute, ebit, resultat_net, tresorerie, dettes_financieres, fonds_propres, capital_souscrit, ratios: {...}}, ...]
schema_type	full | abrege | micro (selon le schema de depot NBB)
last_updated	Date de derniere mise a jour du document

Traitement Spark
Spark lit tous les CSVs en parallele. Pour chaque {bce}/hbb/, il parse les codes PCMN, calcule les ratios et insere ou met a jour le document MongoDB via upsert sur enterprise_number.

Le DAG Airflow declenchera ce recalcul chaque annee quand de nouveaux depots sont disponibles. Seules les entreprises avec de nouveaux exercices sont retraitees (incremental via StateDB).



Part 2  Backend FastAPI
Le backend expose les donnees Gold et Silver au frontend. Il sert les fiches entreprise, les ratios calcules, et orchestre le scraping des documents statuts en streaming SSE.

Ce que le backend doit servir
Fonctionnalite	Description
Recherche entreprise	Retourner une liste d'entreprises correspondant a un nom ou un numero BCE
Fiche entreprise	Infos Silver + ratios Gold pour une entreprise donnee
Statuts notaire (SSE)	Lancer le scraping notaire.be et streamer les documents au fur et a mesure
Dirigeants	Recuperer les dirigeants depuis kbopub (persistes en base apres scrape)

SSE  streaming des statuts
Quand l'utilisateur demande les statuts d'une entreprise, le backend lance le scraper notaire.py via asyncio et diffuse les documents au fur et a mesure de leur recuperation (Server-Sent Events). Le frontend affiche chaque document des reception sans attendre la fin du scraping.

Proxies Tor pour notaire.py
Les requetes vers statuts.notaire.be passent par les proxies Tor (tor1/tor2/tor3 sur ports 9050/9052/9054) via socks5h://. En cas de blocage, le scraper tourne sur le proxy suivant. (si vous avez des soucis avec les notaire, vous pouvez l’ignorer et passer a autre chose)



Part 3  Frontend React
Le frontend est construit avec React + Vite + Redux Toolkit. Il interroge le backend FastAPI et affiche les fiches entreprise avec graphiques et tableaux.

Vues principales
Vue	Contenu
Recherche	Barre de recherche par nom ou BCE, liste de resultats en temps reel
Fiche entreprise	Informations generales, Sankey financier, tableau des ratios par annee, statuts notaire
Statuts notaire	Chargement progressif via SSE avec spinner pendant le streaming

Informations affichees sur la fiche
Section	Source
Nom, forme juridique, statut, adresse	enterprise_silver
Activites NACE avec labels	enterprise_silver
Dirigeants et representants	kbopub (scrape SSE, persiste en base apres scrape)
Ratios financiers par annee	hotel_gold
Sankey compte de resultats	hotel_gold
Statuts et actes notaries	statuts.notaire.be (scrape SSE a la demande)

Sankey compte de resultats
Le Sankey visualise les flux financiers du compte de resultats. Il est construit avec 3 noeuds fixes pour toutes les entreprises, quel que soit le schema de depot (full, abrege ou micro).

Noeud	Valeur
CA (Chiffre d'affaires)	Code PCMN 70
Marge brute	CA - Achats + Variation stocks (60 + 71)
Resultat net	Code PCMN 9904

Le Sankey affiche l'annee selectionnee. Un selecteur permet de naviguer entre les exercices disponibles dans hotel_gold.years.

Gestion du chargement SSE
Pendant le streaming des statuts notaire, le frontend affiche un spinner. Chaque document recu via SSE est ajoute immediatement dans la liste sans attendre la fin du stream. Quand le stream se ferme, le spinner disparait.

Les donnees dirigeants et statuts ne sont scrapees qu'une seule fois. Une fois en base, elles sont servies directement sans relancer le scraper.



Chantier 4  DAG Airflow (recalcul annuel)
Un DAG Airflow gere le recalcul annuel de la couche Gold. Il s'appuie sur StateDB pour ne retraiter que les entreprises ayant de nouveaux depots depuis le dernier passage.

Logique du DAG
Etape	Action
1. Lister les entreprises	Recuperer toutes les entrees StateDB avec status=done
2. Verifier les nouveaux depots	Pour chaque entreprise, comparer filings_done avec ce que NBB expose aujourd'hui
3. Telecharger les nouveaux	Scraper uniquement les exercices absents de filings_done
4. Recalculer Gold	Relancer le job Spark uniquement sur les entreprises mises a jour
5. Upsert MongoDB	Mettre a jour hotel_gold.years pour les entreprises concernees

Les entreprises dont filings_done est deja complet ne sont pas retouchees. Cela permet de relancer le DAG chaque annee sans retraiter l'integralite du dataset.


