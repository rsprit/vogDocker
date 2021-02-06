import os
import numpy as np
import pandas as pd
from Bio import SeqIO


def load_species(data_path):
    filename = os.path.join(data_path, "vog.species.list")
    return pd.read_csv(
        filename,
        sep="\t",
        header=0,
        names=["SpeciesName", "TaxonID", "Phage", "Source", "Version"],
        index_col="TaxonID",
    ).assign(Phage=lambda df: df.Phage == "phage")


def load_members(data_path):
    filename = os.path.join(data_path, "vog.members.tsv.gz")
    return pd.read_csv(
        filename,
        compression="gzip",
        sep="\t",
        header=0,
        names=[
            "VOG_ID",
            "ProteinCount",
            "SpeciesCount",
            "FunctionalCategory",
            "Proteins",
        ],
        usecols=[
            "VOG_ID",
            "ProteinCount",
            "SpeciesCount",
            "FunctionalCategory",
            "Proteins",
        ],
        index_col="VOG_ID",
    ).assign(Proteins=lambda df: df.Proteins.apply(lambda s: set(s.split(","))))


def load_annotations(data_path):
    filename = os.path.join(data_path, "vog.annotations.tsv.gz")
    return pd.read_csv(
        filename,
        compression="gzip",
        sep="\t",
        header=0,
        names=[
            "VOG_ID",
            "ProteinCount",
            "SpeciesCount",
            "FunctionalCategory",
            "Consensus_func_description",
        ],
        usecols=["VOG_ID", "Consensus_func_description"],
        index_col="VOG_ID",
    )


def load_virusonly(data_path):
    filename = os.path.join(data_path, "vog.virusonly.tsv.gz")
    return pd.read_csv(
        filename,
        compression="gzip",
        sep="\t",
        header=0,
        names=["VOG_ID", "StringencyHigh", "StringencyMedium", "StringencyLow"],
        dtype={"StringencyHigh": bool, "StringencyMedium": bool, "StringencyLow": bool},
        index_col="VOG_ID",
    ).assign(
        VirusSpecific=lambda df: df.StringencyHigh
        | df.StringencyMedium
        | df.StringencyLow
    )


def load_lca(data_path):
    filename = os.path.join(data_path, "vog.lca.tsv.gz")
    return pd.read_csv(
        filename,
        compression="gzip",
        sep="\t",
        header=0,
        names=["VOG_ID", "GenomesInGroup", "GenomesTotal", "Ancestors"],
        index_col="VOG_ID",
    )


def load_nt_seq(data_path):
    """
    Loads the nucleotid sequences from FASTA file into a Dataframe
    """
    filename = os.path.join(data_path, "vog.genes.all.fa")
    data = [[s.id, str(s.seq)] for s in SeqIO.parse(filename, "fasta")]
    return pd.DataFrame(data, columns=["ProteinID", "NTseq"]).set_index("ProteinID")


def load_aa_seq(data_path):
    """
    Loads the amino acid sequences from FASTA file into a Dataframe
    """

    filename = os.path.join(data_path, "vog.proteins.all.fa")
    data = [[s.id, str(s.seq)] for s in SeqIO.parse(filename, "fasta")]
    return pd.DataFrame(data, columns=["ProteinID", "AAseq"]).set_index("ProteinID")


def extract_membership(members):
    """
    Loads all vog<->protein relationships.

    :param the members frame
    """
    data = [(v, p) for v, ps in members.Proteins.items() for p in ps]
    return (
        pd.DataFrame.from_records(data, columns=["VOG_ID", "ProteinID"])
        .sort_values(["VOG_ID", "ProteinID"])
        .reset_index(drop=True)
    )


def extract_proteins(membership):
    """
    Extracts all distinct proteins from the membership table and associates them
    with the taxid of their species.
    """
    unique_proteins = pd.Series(membership.ProteinID.unique())
    species = unique_proteins.apply(lambda p: int(p.split(".")[0]))

    return (
        pd.DataFrame({"ProteinID": unique_proteins, "TaxonID": species})
        .sort_values("ProteinID")
        .set_index("ProteinID")
    )


def load_frames(data_path):
    species = load_species(data_path)
    members = load_members(data_path)
    annotations = load_annotations(data_path)
    virusonly = load_virusonly(data_path)
    lca = load_lca(data_path)
    aa_seq = load_aa_seq(data_path)
    nt_seq = load_nt_seq(data_path)

    membership = extract_membership(members)

    proteins = (
        extract_proteins(membership).join(aa_seq, how="left").join(nt_seq, how="left")
    )

    protein_phage = proteins.TaxonID.map(species.Phage.apply(lambda s: 1 if s else 0))

    vog = (
        members.drop(columns="Proteins")
        .join(annotations)
        .join(lca)
        .join(virusonly)
        .assign(
            NumPhages=membership.set_index("VOG_ID")
            .ProteinID.map(protein_phage)
            .groupby("VOG_ID")
            .sum(),
            NumNonPhages=membership.set_index("VOG_ID")
            .ProteinID.map(1 - protein_phage)
            .groupby("VOG_ID")
            .sum(),
            PhageNonphage=lambda df: (
                np.sign(df.NumPhages) + 2 * np.sign(df.NumNonPhages)
            ).map({1: "phages_only", 2: "np_only", 3: "mixed"}),
        )
    )

    return (vog, species, proteins, membership)
