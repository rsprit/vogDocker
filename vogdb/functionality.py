import os
import logging
import gzip
from typing import Dict, Optional, Set, List
from sqlalchemy.orm import Session
from sqlalchemy import func

from .models import Species_profile, VOG_profile, Protein_profile, AA_seq, NT_seq
from .taxa import ncbi_taxa

# get logger:
log = logging.getLogger(__name__)

"""
Here we define all the search methods that are used for extracting the data from the database
"""

"""
Very important Note: Here we specify what columns we want to get from our query: e.g. protein_id,..,species_name
In order that this result output is gonna pass through the Pydantic validation, two criteria need to be valid:
1. the attribute type values of the returned query object (in functionality.py)  (e.g. Species_profile.species_name)
 need to match the attribute type of the Pydantic response model (in this case schemas.Species_profile.species_name)
2. The names of the  attributes the returned query object also need to be exactly the same as in the Pydantic 
response model object, so we have in query object with attribute Protein_profile.species_name
so the pydantic response model (Protein_profile) needs to have the attribute name species_name as well

if those two criteria are not fulfilled, pydantic will throw an ValidationError

"""


def get_species(db: Session,
                taxon_id: List[int],
                species_name: List[str],
                phage: Optional[bool],
                source: Optional[str]):
    """
    This function searches the Species based on the given query parameters
    """
    log.debug("Searching Species in the database...")

    query = db.query(Species_profile.taxon_id)

    if taxon_id:
        query = query.filter(Species_profile.taxon_id.in_(set(taxon_id)))

    if species_name:
        for name in set(species_name):
            query = query.filter(Species_profile.species_name.like("%" + name + "%"))

    if phage is not None:
        query = query.filter(Species_profile.phage == phage)

    if source:
        query = query.filter(Species_profile.source.like("%" + source + "%"))

    return query.order_by(Species_profile.taxon_id).all()


def find_species_by_id(db: Session, ids: List[int]):
    """
    This function returns the Species information based on the given species IDs
    """
    if ids:
        log.debug("Searching Species by IDs in the database...")
        return db.query(Species_profile).filter(Species_profile.taxon_id.in_(ids)).all()
    else:
        log.debug("No IDs were given.")
        return list()


def get_vogs(db: Session,
             id: Optional[Set[str]],
             pmin: Optional[int],
             pmax: Optional[int],
             smax: Optional[int],
             smin: Optional[int],
             function: Optional[Set[str]],
             consensus_function: Optional[Set[str]],
             mingLCA: Optional[int],
             maxgLCA: Optional[int],
             mingGLCA: Optional[int],
             maxgGLCA: Optional[int],
             ancestors: Optional[Set[str]],
             h_stringency: Optional[bool],
             m_stringency: Optional[bool],
             l_stringency: Optional[bool],
             virus_specific: Optional[bool],
             phages_nonphages: Optional[str],
             proteins: Optional[Set[str]],
             species: Optional[Set[str]],
             tax_id: Optional[Set[int]],
             union: Optional[bool]):
    """
    This function searches the VOG based on the given query parameters
    """
    log.info("Searching VOGs in the database...")

    result = db.query(VOG_profile.id)

    # make checks for validity of user input:
    def check_validity(pair):
        min = pair[0]
        max = pair[1]
        if (min is not None) and (max is not None):
            if max < min:
                # ToDo value error.
                raise ValueError("The provided min is greater than the provided max.")
            elif min < 0 or max < 0:
                raise ValueError("Number for min or max cannot be negative!")

    for pair in [[smin, smax], [pmin, pmax], [mingLCA, maxgLCA], [mingGLCA, maxgGLCA]]:
        check_validity(pair)

    for number in smin, smax, pmin, pmax, mingLCA, maxgLCA, mingGLCA, maxgGLCA:
        if number is not None:
            if number < 1:
                raise ValueError('Provided number: %s has to be > 0.' % number)

    # create a warning in the log file if "union" is specified but no species/taxIDs given to use the parameter
    #ToDo: What type of error here?
    if union is True:
        if species is None and tax_id is None:
            log.error("The 'Union' Parameter was provided, but no species or taxonomy IDs were provided.")
            raise Exception("The 'Union' Parameter was provided, but no species or taxonomy IDs were provided.")
        elif species is not None and len(species) < 2:
            log.error("The 'Union' Parameter was provided, but the number of species is smaller than 2.")
            raise Exception("The 'Union' Parameter was provided, but the number of species is smaller than 2.")
        elif tax_id is not None and len(tax_id) < 2:
            log.error("The 'Union' Parameter was provided, but the number of taxonomy IDs is smaller than 2.")
            raise Exception("The 'Union' Parameter was provided, but the number of taxonomy IDs is smaller than 2.")

    if id:
        result = result.filter(VOG_profile.id.in_(id))

    if consensus_function:
        for d in set(consensus_function):
            result = result.filter(VOG_profile.consensus_function.like("%" + d + "%"))

    if function:
        for d in set(function):
            result = result.filter(VOG_profile.function.like("%" + d + "%"))

    if smin is not None:
        result = result.filter(VOG_profile.species_count >= smin)
    if smax is not None:
        result = result.filter(VOG_profile.species_count <= smax)

    if pmin is not None:
        result = result.filter(VOG_profile.protein_count >= pmin)
    if pmax is not None:
        result = result.filter(VOG_profile.protein_count <= pmax)

    if mingLCA is not None:
        result = result.filter(VOG_profile.genomes_total_in_LCA >= mingLCA)
    if maxgLCA is not None:
        result = result.filter(VOG_profile.genomes_total_in_LCA <= maxgLCA)
    
    if mingGLCA is not None:
        result = result.filter(VOG_profile.genomes_in_group >= mingGLCA)
    if maxgGLCA is not None:
        result = result.filter(VOG_profile.genomes_in_group <= maxgGLCA)

    if h_stringency is not None:
        result = result.filter(VOG_profile.h_stringency == h_stringency)    
    if m_stringency is not None:
        result = result.filter(VOG_profile.m_stringency == m_stringency)    
    if l_stringency is not None:
        result = result.filter(VOG_profile.l_stringency == l_stringency)    
    if virus_specific is not None:
        result = result.filter(VOG_profile.virus_specific == virus_specific)    

    if phages_nonphages:
        for d in set(phages_nonphages):
            result = result.filter(VOG_profile.phages_nonphages.like("%" + d + "%"))

    if ancestors:
        for d in set(ancestors):
            result = result.filter(VOG_profile.ancestors.like("%" + d + "%"))

    if proteins:
        for d in set(proteins):
            result = result.filter(VOG_profile.proteins.any(Protein_profile.id == d))

    arguments = locals()
    filters = []
    for key, value in arguments.items():  # type: str, any
        if value is not None:

            # if key == "proteins":
            #     for protein in value:
            #         p = "%" + protein + "%"
            #         filters.append(getattr(VOG_profile, key).like(p))

            if key == "species":
                if union is False:
                    # this is the INTERSECTION SEARCH:
                    vog_ids = db.query().with_entities(Protein_profile.vog_id).join(Species_profile). \
                        filter(Species_profile.species_name.in_(species)).group_by(Protein_profile.vog_id). \
                        having(func.count(Species_profile.species_name) == len(species)).all()
                else:
                    # UNION SEARCH below:
                    vog_ids = db.query().with_entities(Protein_profile.vog_id).join(Species_profile). \
                        filter(Species_profile.species_name.in_(species)).group_by(Protein_profile.vog_id).all()

                vog_ids = {id[0] for id in vog_ids}  # convert to set
                filters.append(VOG_profile.id.in_(vog_ids))

            if key == "tax_id":
                ncbi = ncbi_taxa()
                try:
                    id_list = []
                    if union:
                        # UNION SEARCH:
                        for id in tax_id:
                            id_list.extend(
                                ncbi.get_descendant_taxa(id, collapse_subspecies=False, intermediate_nodes=True))
                            id_list.append(id)
                        vog_ids = db.query().with_entities(Protein_profile.vog_id).join(Species_profile). \
                            filter(Species_profile.taxon_id.in_(id_list)).group_by(Protein_profile.vog_id). \
                            filter(Species_profile.taxon_id.in_(id_list)).group_by(Protein_profile.vog_id).all()
                        vog_ids = {id[0] for id in vog_ids}  # convert to set
                        filters.append(getattr(VOG_profile, "id").in_(vog_ids))
                    else:
                        # INTERSECTION SEARCH:
                        for id in tax_id:
                            id_list1 = []
                            id_list1.extend(
                                ncbi.get_descendant_taxa(id, collapse_subspecies=False, intermediate_nodes=True))
                            id_list1.append(id)
                            vog_ids = db.query().with_entities(Protein_profile.vog_id).join(Species_profile). \
                                filter(Species_profile.taxon_id.in_(id_list1)).group_by(Protein_profile.vog_id). \
                                filter(Species_profile.taxon_id.in_(id_list1)).group_by(Protein_profile.vog_id).all()
                            vog_ids = {id[0] for id in vog_ids}  # convert to set
                            filters.append(getattr(VOG_profile, "id").in_(vog_ids))
                except ValueError:
                    raise ValueError("The provided taxonomy ID is invalid: {0}".format(id))

    return result.filter(*filters).order_by(VOG_profile.id).all()


def find_vogs_by_uid(db: Session, ids: Optional[List[str]]):
    """
    This function returns the VOG information based on the given VOG IDs
    """

    if ids:
        log.debug("Searching VOGs by IDs in the database...")

        return db.query(VOG_profile).filter(VOG_profile.id.in_(ids)).all()
    else:
        log.debug("No IDs were given.")

        return list()


def get_proteins(db: Session,
                 species: List[str],
                 taxon_id: List[int],
                 vog_id: List[str]):
    """
    This function searches the for proteins based on the given query parameters
    """
    log.debug("Searching Proteins in the database...")

    query = db.query(Protein_profile.id)

    if taxon_id:
        query = query.filter(Protein_profile.taxon_id.in_(set(taxon_id)))

    if vog_id:
        query = query.filter(Protein_profile.vog_id.in_(set(vog_id)))

    if species:
        query = query.join(Species_profile)
        for s in set(species):
            query = query.filter(Species_profile.species_name.like("%" + s + "%"))

    return query.order_by(Protein_profile.id).all()


def find_proteins_by_id(db: Session, pids: List[str]):
    """
    This function returns the Protein information based on the given Protein IDs
    """
    if pids:
        log.debug("Searching Proteins by ProteinIDs in the database...")

        return db.query(Protein_profile).filter(Protein_profile.id.in_(pids)).all()
    else:
        log.debug("No IDs were given.")

        return list()


def find_vogs_hmm_by_uid(uid: List[str]) -> Dict[str, str]:
    log.debug("Searching for Hidden Markov Models (HMM) in the data files...")

    if not uid:
        log.debug("No IDs were given.")
        return {}

    return {id:hmm_content(id) for id in set(uid)}


def hmm_content(uid: str) -> str:
    try:
        return _load_gzipped_file_content(uid.upper(), "hmm", ".hmm.gz")
    except FileNotFoundError:
        log.exception(f"No HMM for {uid}")
        raise KeyError(f"Invalid Id {uid}")


def find_vogs_msa_by_uid(uid: List[str]) -> Dict[str,str]:
    log.debug("Searching for Multiple Sequence Alignments (MSA) in the data files...")

    if not uid:
        log.debug("No IDs were given.")
        return {}

    return {id:msa_content(id) for id in set(uid)}


def msa_content(uid: str) -> str:
    try:
        return _load_gzipped_file_content(uid.upper(), "raw_algs", ".msa.gz")
    except FileNotFoundError:
        log.exception(f"No MSA for {uid}")
        raise KeyError(f"Invalid Id {uid}")


def find_protein_faa_by_id(db: Session, id: Optional[List[str]]):
    """
    This function returns the Aminoacid sequences of the proteins based on the given Protein IDs
    """
    if id:
        log.info("Searching AA sequence by ProteinIDs in the database...")

        return db.query(AA_seq).filter(AA_seq.id.in_(id)).all()
    else:
        log.error("No IDs were given.")

        return list()


def find_protein_fna_by_id(db: Session, id: Optional[List[str]]):
    """
    This function returns the Nucleotide sequences of the proteins based on the given Protein IDs
    """
    if id:
        log.info("Searching NT sequence by ProteinIDs in the database...")

        return  db.query(NT_seq).filter(NT_seq.id.in_(id)).all()
    else:
        log.error("No IDs were given.")

        return list()


def _load_gzipped_file_content(id: str, prefix: str, suffix: str) -> str:
    file_name = os.path.join(os.environ.get("VOG_DATA", "data"), prefix, id + suffix)
    with gzip.open(file_name, "rt") as f:
        return f.read()

