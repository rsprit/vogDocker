#!/usr/bin/env python
# coding: utf-8
import os
import sys
import pandas as pd
import numpy as np

from sqlalchemy import create_engine
from Bio import SeqIO
from sqlalchemy import VARCHAR
from sqlalchemy.dialects.mysql import LONGTEXT

"""
Here we create our VOGDB and create all the tables that we are going to use
"""

def generate_db(data_path, db_url):

    # Create an engine object.
    engine = create_engine(db_url)

    with engine.connect() as con:
        con.execute("DROP TABLE IF EXISTS NT_seq;")
        con.execute("DROP TABLE IF EXISTS AA_seq;")
        con.execute("DROP TABLE IF EXISTS Protein_profile;")
        con.execute("DROP TABLE IF EXISTS VOG_profile;")
        con.execute("DROP TABLE IF EXISTS Species_profile;")

    # ---------------------
    # Species_table generation
    # ----------------------
    # extract Species information from the list
    species_list_df = pd.read_csv(data_path + "vog.species.list",
                                  sep='\t',
                                  header=0,
                                  names=['SpeciesName', 'TaxonID', 'Phage', 'Source', 'Version']) \
        .assign(Phage=lambda p: p.Phage == 'phage')
    # create a species table in the database

    species_list_df.to_sql(name='Species_profile', con=engine, if_exists='replace', index=False, chunksize=1000)

    with engine.connect() as con:
        con.execute('ALTER TABLE Species_profile ADD PRIMARY KEY (TaxonID);')
        con.execute('ALTER TABLE Species_profile  MODIFY  SpeciesName char(100) NOT NULL; ')
        con.execute('ALTER TABLE Species_profile  MODIFY  TaxonID int(30) NOT NULL; ')
        con.execute('ALTER TABLE Species_profile  MODIFY  Phage bool NOT NULL; ')
        con.execute('ALTER TABLE Species_profile  MODIFY  Source char(100) NOT NULL; ')
        con.execute('ALTER TABLE Species_profile  MODIFY  Version int(255) NOT NULL; ')

    # ToDo add foreign key to connect tax_id in protein_profile and species_profile? create index?

    print('Species_profile table successfully created!')


    # ---------------------
    # VOG_table generation
    # ----------------------
    members = pd.read_csv(data_path + "vog.members.tsv.gz", compression='gzip',
                          sep='\t',
                          header=0,
                          names=['VOG_ID', 'ProteinCount', 'SpeciesCount', 'FunctionalCategory', 'Proteins'],
                          usecols=['VOG_ID', 'ProteinCount', 'SpeciesCount', 'FunctionalCategory', 'Proteins'],
                          index_col='VOG_ID')

    annotations = pd.read_csv(data_path + "vog.annotations.tsv.gz", compression='gzip',
                              sep='\t',
                              header=0,
                              names=['VOG_ID', 'ProteinCount', 'SpeciesCount', 'FunctionalCategory', 'Consensus_func_description'],
                              usecols=['VOG_ID', 'Consensus_func_description'],
                              index_col='VOG_ID')

    #lca = pd.read_csv(os.path.join(data_path, 'vog.lca.tsv.gz'), compression='gzip',
    lca = pd.read_csv(data_path + 'vog.lca.tsv.gz', compression='gzip',
                      sep='\t',
                      header=0,
                      names=['VOG_ID', 'GenomesInGroup', 'GenomesTotal', 'Ancestors'],
                      index_col='VOG_ID')

    virusonly = pd.read_csv(data_path + 'vog.virusonly.tsv.gz', compression='gzip',
                            sep='\t',
                            header=0,
                            names=['VOG_ID', 'StringencyHigh', 'StringencyMedium', 'StringencyLow'],
                            dtype={'StringencyHigh': bool, 'StringencyMedium': bool, 'StringencyLow': bool},
                            index_col='VOG_ID')

    dfr = members.join(annotations).join(lca).join(virusonly)
    dfr['VirusSpecific'] = np.where((dfr['StringencyHigh']
                                    | dfr['StringencyMedium']
                                    | dfr['StringencyLow'])
                                    , True, False)


    #create num of phages and non-phages for VOG. also "phages_only" "np_only" or "mixed"
    dfr['NumPhages'] = 0
    dfr['NumNonPhages'] = 0
    dfr['PhageNonphage'] = ''

    species_list_df.set_index("TaxonID", inplace=True)
    for index, row in dfr.iterrows():
        num_nonphage = 0
        num_phage = 0
        p = row['Proteins'].split(",")
        for protein in p:
            species_id = protein.split('.')[0]
            species_id = int(species_id)
            if (species_list_df.loc[species_id])["Phage"]:
                num_phage = num_phage + 1
            else:
                num_nonphage = num_nonphage + 1

        dfr.at[index, 'NumPhages'] = num_phage
        dfr.at[index, 'NumNonPhages'] = num_nonphage

        if ((num_phage > 0) and (num_nonphage > 0)):
            dfr.at[index, 'PhageNonphage'] = "mixed"
        elif (num_phage > 0):
            dfr.at[index, 'PhageNonphage'] = "phages_only"
        else:
            dfr.at[index, 'PhageNonphage'] = "np_only"


    # create a table in the database
    dfr.to_sql(name='VOG_profile', con=engine, if_exists='replace', index=True,
               dtype={'VOG_ID': VARCHAR(30), 'Proteins': LONGTEXT},
               chunksize=1000)

# ToDo: VOG needs to be capitalized
    with engine.connect() as con:
        con.execute('ALTER TABLE VOG_profile ADD PRIMARY KEY (VOG_ID); ')  # add primary key
        con.execute('ALTER TABLE VOG_profile  MODIFY  VOG_ID char(30) NOT NULL; ')  # convert text to char
        con.execute('ALTER TABLE VOG_profile  MODIFY  FunctionalCategory char(30) NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  Consensus_func_description char(100) NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  ProteinCount int(255) NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  SpeciesCount int(255) NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  GenomesInGroup int(255) NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  GenomesTotal int(255) NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  Ancestors TEXT; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  StringencyHigh bool NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  StringencyMedium bool NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  StringencyLow bool NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  VirusSpecific bool NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  NumPhages int(255) NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  NumNonPhages int(255) NOT NULL; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  PhageNonphage TEXT; ')
        con.execute('ALTER TABLE VOG_profile  MODIFY  Proteins LONGTEXT; ')
        con.execute('CREATE UNIQUE INDEX vog_profile_index ON VOG_profile (VOG_ID, FunctionalCategory);')
        # con.execute('CREATE INDEX VOG_profile_index2 ON VOG_profile (Consensus_func_description);')  # create index
        #con.execute('ALTER TABLE VOG_profile  ADD FOREIGN KEY (TaxonID) REFERENCES Species_profile(TaxonID); ')
    # ToDo: add foreign keys to link to proteins, and species lists.
    print('VOG_table successfully created!')


    #---------------------
    # Protein_table generation
    #----------------------

    # extract proteins for each VOG
    protein_list_df = pd.read_csv(data_path + "vog.members.tsv.gz", compression='gzip', sep='\t').iloc[:, [0, 4]]

    protein_list_df = protein_list_df.rename(columns={"#GroupName": "VOG_ID", "ProteinIDs": "ProteinID"})

    # assign each protein a vog
    protein_list_df = (protein_list_df["ProteinID"].str.split(",").apply(lambda x: pd.Series(x))
                       .stack()
                       .reset_index(level=1, drop=True)
                       .to_frame("ProteinID")
                       .join(protein_list_df[["VOG_ID"]], how="left")
                       )
    protein_list_df.set_index("ProteinID")

    # separate protein and taxonID into separate columns
    protein_list_df["TaxonID"] = protein_list_df["ProteinID"].str.split(".").str[0]
    #protein_list_df["ProteinID"] = protein_list_df["ProteinID"].str.split(".").str[1:3].str.join(".")

    # create a protein table in the database
    protein_list_df.to_sql(name='Protein_profile', con=engine, if_exists='replace', index=False, chunksize=1000)

#Todo: Protein_profile needs to be capitalized
    with engine.connect() as con:
        con.execute('ALTER TABLE Protein_profile  MODIFY  ProteinID char(30) NOT NULL; ')
        con.execute('ALTER TABLE Protein_profile  MODIFY  TaxonID int(30) NOT NULL; ')
        con.execute('ALTER TABLE Protein_profile  MODIFY  VOG_ID char(30) NOT NULL; ')
        con.execute('CREATE INDEX vog_profile_index_by_protein ON Protein_profile (ProteinID);')
        # add foreign key
        con.execute('ALTER TABLE Protein_profile  ADD FOREIGN KEY (TaxonID) REFERENCES Species_profile(TaxonID); ')
        con.execute('ALTER TABLE Protein_profile  ADD FOREIGN KEY (VOG_ID) REFERENCES VOG_profile(VOG_ID); ')

    print('Protein_profile table successfully created!')
    
    #---------------------
    # Amino Acid and Nucleotide Sequence Table Generation
    #----------------------

    proteinfile = data_path + "vog.proteins.all.fa"
    prot = []
    for seq_record in SeqIO.parse(proteinfile, "fasta"):
        prot.append([seq_record.id, str(seq_record.seq)])
    df = pd.DataFrame(prot, columns=['ID', 'AAseq'])
    df.set_index("ID") 
    
    # convert dataframe to DB Table:
    df.to_sql(name='AA_seq', con=engine, if_exists='replace', index=False, chunksize=1000)    
    
    with engine.connect() as con:
        con.execute('ALTER TABLE AA_seq MODIFY ID char(30) NOT NULL;')
        con.execute('ALTER TABLE AA_seq MODIFY AASeq LONGTEXT;')
        con.execute('CREATE INDEX ID ON AA_seq (ID);')
        
    print('Amino-acid sequences table successfully created!')     
        
    
    genefile = data_path + "vog.genes.all.fa"
    genes = []
    for seq_record in SeqIO.parse(genefile, "fasta"):
        genes.append([seq_record.id, str(seq_record.seq)])
    dfg = pd.DataFrame(genes, columns=['ID', 'NTseq'])
    dfg.set_index('ID')
    
    # convert dataframe to DB table:
    dfg.to_sql(name='NT_seq', con=engine, if_exists='replace', index=False, chunksize=1000)

    with engine.connect() as con:
        con.execute('ALTER TABLE NT_seq MODIFY ID char(30) NOT NULL;')
        con.execute('ALTER TABLE NT_seq MODIFY NTSeq LONGTEXT;')
        con.execute('CREATE INDEX ID ON NT_seq (ID);')

    print('Nucleotide sequences table successfully created!')     
