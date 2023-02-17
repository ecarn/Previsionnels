import datetime
import os
import Metiers.Bdd
import Models.Commande
import pandas as pd
import shutil
from collections import defaultdict
import Metiers.Convertisseur as converts
import Metiers.Bdd as metierBDD
import Metiers.Mail as Mails
import pymsteams
import subprocess

alerteADVTM = pymsteams.connectorcard("https://vabelfr.webhook.office.com/webhookb2/99984ba2-dc01-4017-a9a8-1c3e29e051a9@08914632-4049-4153-a530-2be93da933aa/IncomingWebhook/f166b4a0628f45a6a92f8202d45665a7/212e74de-d1d7-430d-8608-affe0a0f5032")
repImport = r"S:\Planning\Planning - SUPPLY CHAIN\charge\PREV\import auto"
repSave = r"S:\Planning\Planning - SUPPLY CHAIN\charge\PREV\import auto\save"
s = "CHANEL : \n"
dests = "jennifer.sellier@vabel-parfums.fr,melanie.queval@vabel-parfums.fr,projet@vabel-parfums.fr,nina.jonville@vabel-parfums.fr,adv4@vabel-parfums.fr,informatique@vabel-parfums.fr"

articlesManquants = ""

def print_hi():
    for files in os.listdir(repImport):

        if files.endswith(".xlsx"):
            shutil.copy2(repImport + '/' + files, repSave + '/' + files)
            filename = files.split('_')[0]
            if filename == "JHG":
                #traiteFichiersJHG(files)
                print("JHG")
            elif filename == "CHA":
                traiteFichiersCHA(files)
                print("CHA")
            elif filename == "BRI":
                #traiteFichiersBRI(files)
                print("BRI")
            else:
                print("Err")

            print(os.path.join(repImport, files))

def traiteFichiersBRI(file):
    #Recup du fichier en dataframe
    xl_file = pd.ExcelFile(repImport + '/' + file)
    df = xl_file.parse("MAQ")

    #Changement des headers pour y mettre les dates
    new_header = df.iloc[0]  # grab the first row for the header
    df = df[1:]  # take the data less the header row
    df.columns = new_header  # set the header row as the df header

    listeCommandes = []

    #On récupère la liste des prevs dans PMI
    listeCommandesPMI = Metiers.Bdd.getListePrevsPMI("BRI")
    lstManquants = []
    #On parcourt les lignes restantes
    for index, row in df.iterrows():
        for column in df.columns:
            if str(column).__contains__('-'):
                if not metierBDD.checkArticleExists(str(row[0])) and not lstManquants.__contains__(str(row[0]))and str(row[0]) != 'nan':
                    lstManquants.append(str(row[0]))
                    f = open(r"\\srv-ad\services\Planning\Planning - SUPPLY CHAIN\charge\PREV\import "
                             r"auto\importPrevManquants.txt", "a")
                    f.write(str(row[0]) + '\n')
                    f.close()
                else:
                    codeArticle = metierBDD.getLastArticle(str(row[0]), "BRI")

                    qte = 0
                    try:
                        qte = int(str(row[column]))
                        if qte != 0 and codeArticle.lower() != 'nan':
                            listeCommandes.append(
                            Models.Commande.commande("", "", "", codeArticle, "BRI", qte, column.strftime('%Y%m') + "15"))
                    except:
                        pass


    #pour chaque commande dans xls
    for commande in listeCommandes:
        #on regarde si la commande a une corespondance codart/qte/date
        shortWords = [x for x in listeCommandesPMI if x.codart.strip() == commande.codart.strip() and str(x.comart).strip() == str(commande.comart).strip() and str(x.date).strip() == str(commande.date).strip() ]

        if shortWords:
            if str(shortWords[0].quantite).split('.')[0].strip() == str(commande.quantite).strip():

                listeCommandes.remove(commande)
                print("rien a faire")

            else:
                metierBDD.supprimerPrev(shortWords[0])
                print("supprimer la commande")

    for commande in listeCommandes:
        if commande.quantite != 0:
            content = "PREVBRI" + str(commande.date[4:6]) + str(commande.date[:4]) + ";" + commande.codart + ';' + commande.date + ";" + str(commande.quantite) + ";BRI;100041" + '\n'
            f = open(r"\\srv-ad\services\Planning\Planning - SUPPLY CHAIN\charge\PREV\import auto\importPrev.txt",
                     "a")
            f.write(content)
            f.close()

    if len(lstManquants) > 0:
        alerteADVTM.text(r"ARTICLES BRI INCONNUS - ! NON IMPORTES ! :" + "   \n"+ str(lstManquants))

        alerteADVTM.send()

def traiteFichiersCHA(file):
    #Recup du fichier en dataframe
    xl_file = pd.ExcelFile(repImport + '/' + file)
    df = xl_file.parse("PREVSTT")

    toto = df.index[df['CHANEL Parfum Beauté - Planning prévisionnel de commandes sur 12 mois glissants'] == 'Fam. produits'].tolist()
    df = df.iloc[int(toto[0]):, :]

    #Changement des headers pour y mettre les dates
    new_header = df.iloc[0]  # grab the first row for the header
    df = df[1:]  # take the data less the header row
    df.columns = new_header  # set the header row as the df header
    listeCommandes = []

    #On récupère la liste des prevs dans PMI
    listeCommandesPMI = Metiers.Bdd.getListePrevsPMI("CHA")
    lstManquants = []

    #On parcourt les lignes restantes
    for index, row in df.iterrows():
        if str(row[4]) == 'PREV':
          for column in df.columns:
            if str(column).__contains__('-'):
                if not metierBDD.checkArticleExists(str(row[2])) and not lstManquants.__contains__(str(row[2])) and str(row[2]) != 'nan':
                    lstManquants.append(str(row[2]))
                    f = open(r"\\srv-ad\services\Planning\Planning - SUPPLY CHAIN\charge\PREV\import "
                             r"auto\importPrevManquants.txt", "a")
                    f.write(str(row[2]) + '\n')
                    f.close()
                else:
                    codeArticle = metierBDD.getLastArticle(str(row[2]), "CHA")

                    qte = 0
                    try:
                        qte = int(str(row[column]))
                        if qte != 0 and codeArticle.lower() != 'nan':
                            listeCommandes.append(
                                Models.Commande.commande("", "", "", codeArticle, "CHA", qte,
                                                         Metiers.Convertisseur.getDateTime(column)))
                    except:
                        pass

    for commande in listeCommandes:
        shortWords = [x for x in listeCommandesPMI if
                      x.codart.strip() == commande.codart.strip() and str(x.comart).strip() == str(
                          commande.comart).strip() and str(x.date).strip() == str(commande.date).strip()]

        print(commande.codart)
        if shortWords:
            if str(shortWords[0].quantite).split('.')[0].strip() == str(commande.quantite).strip():
                listeCommandes.remove(commande)
                print("rien a faire")

            else:
                #metierBDD.supprimerPrev(shortWords[0])
                print("supprimer la commande")

    for commande in listeCommandes:
        if commande.quantite != 0:
            content = "PREVCHA" + str(commande.date[4:6]) + str(commande.date[:4]) + ";" + commande.codart + ';' + commande.date + ";" + str(commande.quantite) + ";CHA;100004" + '\n'
            f = open(r"\\srv-ad\services\Planning\Planning - SUPPLY CHAIN\charge\PREV\import auto\importPrev.txt",
                     "a")
            f.write(content)
            f.close()
    if len(lstManquants) > 0:
        alerteADVTM.text(r"ARTICLES CHA INCONNUS - ! NON IMPORTES ! :" + "   \n" + str(lstManquants))

        alerteADVTM.send()

def traiteFichiersJHG(file):
    if file.split('_')[0] == "JHG":
        xl_file = pd.ExcelFile(repImport + '/' + file)
        df = xl_file.parse("Feuil1")

        # Nettoyage des row/col inutiles
        i, j = (df.applymap(lambda x: str(x).startswith('REFERENCE'))).values.nonzero()
        t = list(zip(i, j))

        for numb in range(int(j)):
            df.drop(df.columns[0], axis=1, inplace=True)
        for numb in range(int(i)):
            df.drop(df.index[0], axis=0, inplace=True)

        # Noms de colonne
        new_header = df.iloc[0]  # grab the first row for the header
        df = df[1:]  # take the data less the header row
        df.columns = new_header  # set the header row as the df header

        #Liste ou on stokera les prev a inserer
        listeCommandes = []
        # On récupère la liste des prevs dans PMI actuellement
        listeCommandesPMI = Metiers.Bdd.getListePrevsPMI("JHG")
        #création d'une liste pour stocker les articles manquants
        lstManquants = []

        # On parcourt les lignes restantes
        for index, row in df.iterrows():
            for column in df.columns:
                if str(column).__contains__('-'):
                    if not metierBDD.checkArticleExists(str(row[0])) and not lstManquants.__contains__(str(row[0])) and str(row[0]) != 'nan':
                        lstManquants.append(str(row[0]))
                        f = open(r"\\srv-ad\services\Planning\Planning - SUPPLY CHAIN\charge\PREV\import "
                                 r"auto\importPrevManquants.txt", "a")
                        f.write(str(row[0]) + '\n')
                        f.close()
                    else:
                        codeArticle = metierBDD.getLastArticle(str(row[0]), "JHG")

                        qte = 0
                        try:
                            qte = int(str(row[column]))
                            if qte != 0 and codeArticle.lower() != 'nan':
                                listeCommandes.append(
                                    Models.Commande.commande("", "", "", codeArticle, "JHG", qte,
                                                             column.strftime('%Y%m%d')))
                        except:
                            pass

        for commande in listeCommandes:
            shortWords = [x for x in listeCommandesPMI if
                          x.codart.strip() == commande.codart.strip() and str(x.comart).strip() == str(
                              commande.comart).strip() and str(x.date).strip() == str(commande.date).strip()]

            print(commande.codart)
            if shortWords:
                if str(shortWords[0].quantite).split('.')[0].strip() == str(commande.quantite).strip():
                    listeCommandes.remove(commande)
                    print("rien a faire")

                else:
                    metierBDD.supprimerPrev(shortWords[0])
                    print("supprimer la commande")

        for commande in listeCommandes:
            if commande.quantite != 0:
                content = "PREVJHG" + + str(commande.date[4:6]) + str(commande.date[:4]) + ";" + commande.codart + ';' + commande.date + ";" + str(
                    commande.quantite) + ";JHG;100238" + '\n'
                f = open(r"\\srv-ad\services\Planning\Planning - SUPPLY CHAIN\charge\PREV\import auto\importPrev.txt",
                         "a")
                f.write(content)
                f.close()

        if len(lstManquants) > 0:
            alerteADVTM.text(r"ARTICLES JHG INCONNUS - ! NON IMPORTES ! :" + "   \n" + str(lstManquants))
            alerteADVTM.send()

print("efbuefhe")
print_hi()
subprocess.call('"C:\Program Files (x86)\Cegid\ManufacturingPMI\ManufacturingPMI.exe" USER=NIEC SOC=200 LIBRE1=IMPORTPREV PROG=EDIPARAM')