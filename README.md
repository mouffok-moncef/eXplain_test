### Contexte

Ce test consiste à concevoir et implémenter un prototype d’un système de recherche dans des fichiers *JSON*. 

Les spécifications fonctionnelles du test sont de répondre à des requêtes à partir de champs des fichiers *JSON* au travers d'une **API REST**.

### Raisonnement

L'idée est de faire la migration des fichiers *JSON* contenus dans un bucket s3 vers une base *NoSQL* orienté document.

Pour ce test, le framework *ElasticSearch* a été choisi. Car ce dernier, permet le stockage de documents en format *JSON* et fournit un bon système de requêtage. De plus, le framework est scalable, résiliant et a un temps de réponse aux requêtes records. Enfin, le framework fournit une excellente intégration avec le language *Python*.

### Conception

Le script de migration des données *'migration\_s3\_elasticsearch.py'* comporte deux fonctions principales : 

* Une fonction de création d'un index *ElasticSearch* qui va accueillir les données.
* Une fonction de migration de données qui parcourt les objets du bucket s3 et ingère les données vers l'index *ElasticSearch*.

Pour la conception de l'**API REST** dans le script *'elastic\_api\_rest.py'* le framework *Flask* a été utilisé. Les fonctions *get* des *API* font la traduction des requêtes *API* vers des requêtes *ElasticSearch* pour ensuite les exécuter et retourner les résultats.

Enfin, dans ce repository est fourni un script python *'using\_api\_exemples.py'* avec des exemples d'utilisation de l'**API REST**. Il faut bien sûr d'abord lancer le script de l'**API REST** 
`python elastic_api_rest.py`

### Dépendances 

* Copier les *Access key ID* et *Secret access key* dans le fichier `~/.aws/credentials` pour la connexion au bucket. 

* Faire l'installation d'*ElasticSearch* en local ou utiliser un serveur *ElasticSearch* distant (voir `python script.py --help` dans ce dernier cas pour changer le *host*). 

* Faire l'installation des librairies python suivantes : **boto3, elasticsearch, flask-restful**

