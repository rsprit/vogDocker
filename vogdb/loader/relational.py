from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, Boolean, Text

"""
Here we create our VOGDB and create all the tables that we are going to use
"""


def save_db_sql(db_url, vog, species, proteins, membership):

    # Create an engine object.
    engine = create_engine(db_url)

    with engine.connect() as con:
        # V1 leftovers
        con.execute("DROP TABLE IF EXISTS NT_seq;")
        con.execute("DROP TABLE IF EXISTS AA_seq;")
        con.execute("DROP TABLE IF EXISTS Protein_profile;")
        con.execute("DROP TABLE IF EXISTS VOG_profile;")
        con.execute("DROP TABLE IF EXISTS Species_profile;")
        # V2
        con.execute("DROP TABLE IF EXISTS Member;")
        con.execute("DROP TABLE IF EXISTS Protein;")
        con.execute("DROP TABLE IF EXISTS VOG;")
        con.execute("DROP TABLE IF EXISTS Species;")

    # ---------------------
    # VOG_table generation
    # ----------------------

    # create a table in the database
    vog.reset_index().to_sql(
        name="VOG",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        dtype={
            "VOG_ID": String(30),
            "FunctionalCategory": String(30),
            "Consensus_func_description": String(100),
            "ProteinCount": Integer,
            "SpeciesCount": Integer,
            "GenomesInGroup": Integer,
            "GenomesTotal": Integer,
            "Ancestors": String(255),
            "StringencyHigh": Boolean,
            "StringencyMedium": Boolean,
            "StringencyLow": Boolean,
            "VirusSpecific": Boolean,
            "NumPhages": Integer,
            "NumNonPhages": Integer,
            "PhageNonphage": String(32),
        },
    )

    with engine.connect() as con:
        con.execute(
            """
        ALTER TABLE VOG
            MODIFY VOG_ID varchar(30) NOT NULL PRIMARY KEY,
            MODIFY FunctionalCategory varchar(30) NOT NULL,
            MODIFY Consensus_func_description varchar(100) NOT NULL,
            MODIFY ProteinCount int NOT NULL,
            MODIFY SpeciesCount int NOT NULL,
            MODIFY GenomesInGroup int NOT NULL,
            MODIFY GenomesTotal int NOT NULL,
            MODIFY Ancestors varchar(255) NULL,
            MODIFY StringencyHigh bool NOT NULL,
            MODIFY StringencyMedium bool NOT NULL,
            MODIFY StringencyLow bool NOT NULL,
            MODIFY VirusSpecific bool NOT NULL,
            MODIFY NumPhages int NOT NULL,
            MODIFY NumNonPhages int NOT NULL,
            MODIFY PhageNonphage varchar(32) NOT NULL;
        """
        )

    print("VOG table created!")

    # ---------------------
    # Species generation
    # ----------------------

    species.reset_index().to_sql(
        name="Species",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        dtype={
            "TaxonId": Integer,
            "SpeciesName": String(100),
            "Phage": Boolean,
            "Source": String(100),
            "Version": Integer,
        },
    )

    with engine.connect() as con:
        con.execute(
            """
        ALTER TABLE Species
            MODIFY TaxonID int NOT NULL PRIMARY KEY,
            MODIFY SpeciesName varchar(100) NOT NULL,
            MODIFY Phage bool NOT NULL,
            MODIFY Source varchar(100) NOT NULL,
            MODIFY Version int NOT NULL;
        """
        )

    print("Species table created!")

    # ---------------------
    # Protein generation
    # ----------------------

    proteins.reset_index().to_sql(
        name="Protein",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        dtype={
            "ProteinID": String(30),
            "TaxonID": Integer,
            "AAseq": Text(65000),
            "NTseq": Text(65000),
        },
    )

    with engine.connect() as con:
        con.execute(
            """
        ALTER TABLE Protein
            MODIFY ProteinID varchar(30) NOT NULL PRIMARY KEY,
            MODIFY TaxonID int NOT NULL,
            MODIFY AAseq text NULL,
            MODIFY NTseq text NULL,
            ADD FOREIGN KEY(TaxonID) REFERENCES Species(TaxonID);
        """
        )

    print("Protein table created!")

    # ---------------------
    # Member generation
    # ----------------------

    membership.reset_index().to_sql(
        name="Member",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        dtype={"VOG_ID": String(30), "ProteinID": String(30)},
    )

    with engine.connect() as con:
        con.execute(
            """
        ALTER TABLE Member  
            MODIFY VOG_ID varchar(30) NOT NULL,
            MODIFY ProteinID varchar(30) NOT NULL,
            ADD PRIMARY KEY(VOG_ID, ProteinID),
            ADD FOREIGN KEY(VOG_ID) REFERENCES VOG(VOG_ID),
            ADD FOREIGN KEY(ProteinID) REFERENCES Protein(ProteinID);
        """
        )

    print("Member table created!")

    with engine.connect() as con:
        con.execute("OPTIMIZE LOCAL TABLE VOG, Species, Protein, Member;")

    print("All tables optimized!")
