# -*- coding: utf-8 -*-
"""
Created on Tue Dec 21 17:49:31 2021

@author: dell
"""

import psycopg2
import pygrametl

class pays ():
    def __init__(self):
        self.id=""
        self.iso=""
        self.name=""
    def nblignes(self,f):
        mon_fichier = open(f,'r')

        s = 0
        for L in mon_fichier.readlines():
            s+= 1
        mon_fichier.close()
        return s;

    def recuppays(self,fich,):
        f=open(fich,"r")  
        y=0
        h=pays.nblignes(self,fich)
        Pays=[]
        for i in range(h):
            x=[]
            contenu=f.readline()
            if (y==0):
                contenu=contenu[3:]
                y=1
            contenu=contenu.split(';')
            self.id=contenu[0]
            self.id=self.id[1:-1]
            self.iso=contenu[1]
            self.iso=self.iso[:2]
            self.name=contenu[2]
            self.name=self.name[1:-2]
            x.append(self.id)
            x.append(self.iso)
            x.append(self.name)
            #print("l id est :" , self.id , 'et le nom est : ' , self.name , " et l'iso : " ,self.iso)
            Pays.append(x)
        Pays[-1][-1]=Pays[-1][-1]+"e"
        f.close()
        return Pays
    def nompays (self,l,id):
        for i in l :
            if (i[0]==str(id)):
                return (i[2])
        return ("erreur")
p=pays()
x=p.nblignes("pays.txt")
#print(x)
pp=p.recuppays("pays.txt")
#print(p2)
#print(pp)

#ConnexionALaBaseDeDonnées
pgconn=psycopg2.connect(dbname='vente',user='postgres',password='Sassou7600')
connection=pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to "ventes"')
print("connexion établie")

#Schéma Logique Du DW
from pygrametl.tables import Dimension,FactTable
fournisseurs_Dim=Dimension(name=r"fournisseurs",key=r"idfournisseurs",attributes=[r"adressefournisseurs","paysfournisseurs","nomfournisseurs"])
localisationlivraison_Dim=Dimension(name="localisationlivraison",key="idpayslivraison",attributes=["nompayslivraison"])
Temps_dim=Dimension(name="temps",key="idtemps",attributes=["annee","mois","trimestre"])
ventes_fact=FactTable(name="ventesfact",keyrefs=[r"idfournisseurs",r"idpayslivraison",r"idtemps"],measures=["quantite","montant"])

#Extraction et mapping
from pygrametl.datasources import CSVSource
vente_file=open("Livraisons.csv","r")
vente_Source=CSVSource(vente_file,delimiter=";") #listededictionnaires

#temps_dim
love=[]
for row in vente_Source:
    
#fournisseurs_dim
    if(row["DateLivraison"]!=""):
        if (('@' in row["AddressFournisseur"])or (row["AddressFournisseur"]=='unknown')or(row["idPays"]=='0')):
            pass
        else:
            row["adressefournisseurs"]=row["AddressFournisseur"]
            row["paysfournisseurs"]=p.nompays(pp,row["idPays"])
            row["nomfournisseurs"]=row["NomFournisseur"]
            row["idfournisseurs"]=row["id"]
            love.append(fournisseurs_Dim.ensure(row))

          

#localisationlivraison_Dim_dim  
    if(row["DateLivraison"]!=""): 
        row["idpayslivraison"]=row["idPaysLivraison"]
        row["nompayslivraison"]=p.nompays(pp,row["idPaysLivraison"])
        localisationlivraison_Dim.ensure(row)
#temps_dim
    if(row["DateLivraison"]!=""):
        row["annee"]=row["DateLivraison"].split('/')[2][0:4]
        row["mois"]=row["DateLivraison"].split('/')[1]
        if ((row["mois"] =='01') or (row["mois"]=='02') or (row["mois"]=='03')) :
            row["trimestre"]='Q1'
        elif ((row["mois"] =='04') or (row["mois"]=='05') or (row["mois"]=='06')) :
            row["trimestre"]='Q2'
        elif ((row["mois"] =='07') or (row["mois"]=='08') or (row["mois"]=='09')) :
            row["trimestre"]='Q3'
        else:
            row["trimestre"]='Q4'
        row["idtemps"]=row["annee"]+row["mois"]
        Temps_dim.ensure(row)
#ventes_fact
        
        row["quantite"]=float(row["Qte"])
        print("ok1")
        row["montant"]=float(row["Qte"])*float(row["PrixCommande"])
        print("ok2")
        print(row)
        if(row["id"] in love):
            ventes_fact.ensure(row)
        print("ok3")
    
connection.commit()
connection.close()
pgconn.close()
print("ok")
print(love)
