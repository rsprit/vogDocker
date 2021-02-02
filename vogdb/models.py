from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Boolean, Integer, String, Text
from sqlalchemy.orm import relationship
from .database import Base

"""
"model" refers to classes and instances that interact with the database.
A model is equivalent to a database table e.g. VOG_profile table and it contains all the same attributes
"""


class VOG_profile(Base):
    # mysql table name
    __tablename__ = "VOG_profile"

    id = Column('VOG_ID', String(30), primary_key=True)
    protein_count = Column('ProteinCount', Integer, nullable=False)
    species_count = Column('SpeciesCount', Integer, nullable=False)
    function = Column('FunctionalCategory', String(30), nullable=False)
    consensus_function = Column('Consensus_func_description', String(100), nullable=False)
    genomes_in_group = Column('GenomesInGroup', Integer, nullable=False)
    genomes_total_in_LCA = Column('GenomesTotal', Integer, nullable=False)
    ancestors = Column('Ancestors', String(255), nullable=True)
    h_stringency = Column('StringencyHigh', Boolean, nullable=False)
    m_stringency = Column('StringencyMedium', Boolean, nullable=False)
    l_stringency = Column('StringencyLow', Boolean, nullable=False)
    virus_specific = Column('VirusSpecific', Boolean, nullable=False)
    num_phages = Column('NumPhages', Integer, nullable=False)
    num_nonphages = Column('NumNonPhages', Integer, nullable=False)
    phages_nonphages = Column('PhageNonphage', String(32), nullable=False)
    #proteins = Column('Proteins', Text(200000))

    proteins = relationship('Protein_profile', back_populates='vog', lazy='selectin')


class Species_profile(Base):
    # mysql table name
    __tablename__ = "Species_profile"

    taxon_id = Column('TaxonID', Integer, primary_key=True)
    species_name = Column('SpeciesName', String(100), nullable=False)
    phage = Column('Phage', Boolean, nullable=False)
    source = Column('Source', String(100), nullable=False)
    version = Column('Version', Integer, nullable=False)

    proteins = relationship("Protein_profile", back_populates="species", lazy="selectin")


class Protein_profile(Base):
    # mysql table name
    __tablename__ = "Protein_profile"

    id = Column('ProteinID', String(30), nullable=False, index=True, primary_key=True)
    vog_id = Column('VOG_ID', String(30), ForeignKey("VOG_profile.VOG_ID"), nullable=False, index=True, primary_key=True)
    taxon_id = Column('TaxonID', Integer,  ForeignKey("Species_profile.TaxonID"), nullable=False, index=True)

    vog = relationship("VOG_profile", back_populates="proteins", lazy="joined")
    species = relationship("Species_profile", back_populates="proteins", lazy="joined")


class AA_seq(Base):
    # mysql table name
    __tablename__ = "AA_seq"

    id = Column('ID', String(30), primary_key=True)
    seq = Column('AAseq', Text(65000))


class NT_seq(Base):
    # mysql table name
    __tablename__ = "NT_seq"

    id = Column('ID', String(30), primary_key=True)
    seq = Column('NTseq', Text(65000))
