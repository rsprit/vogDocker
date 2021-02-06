import os
import logging
import gzip
from typing import Dict, Optional, Set, List
from sqlalchemy.orm import Session
from sqlalchemy import func

from .models import Species, VOG, Protein, Member
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

    query = db.query(Species.taxon_id)

    if taxon_id:
        query = query.filter(Species.taxon_id.in_(set(taxon_id)))

    if species_name:
        for name in set(species_name):
            query = query.filter(Species.species_name.like("%" + name + "%"))

    if phage is not None:
        query = query.filter(Species.phage == phage)

    if source:
        query = query.filter(Species.source.like("%" + source + "%"))

    return query.order_by(Species.taxon_id).all()


def find_species_by_id(db: Session, ids: List[int]):
    """
    This function returns the Species information based on the given species IDs
    """
    if ids:
        log.debug("Searching Species by IDs in the database...")
        return db.query(Species).filter(Species.taxon_id.in_(ids)).all()
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

    result = db.query(VOG.id)

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
    if union:
        if species is None and tax_id is None:
            log.error("The 'Union' Parameter was provided, but no species or taxonomy IDs were provided.")
            raise ValueError("The 'Union' Parameter was provided, but no species or taxonomy IDs were provided.")
        elif species and len(species) < 2:
            log.error("The 'Union' Parameter was provided, but the number of species is smaller than 2.")
            raise ValueError("The 'Union' Parameter was provided, but the number of species is smaller than 2.")
        elif tax_id and len(tax_id) < 2:
            log.error("The 'Union' Parameter was provided, but the number of taxonomy IDs is smaller than 2.")
            raise ValueError("The 'Union' Parameter was provided, but the number of taxonomy IDs is smaller than 2.")

    if id:
        result = result.filter(VOG.id.in_(id))

    if consensus_function:
        for d in set(consensus_function):
            result = result.filter(VOG.consensus_function.like("%" + d + "%"))

    if function:
        for d in set(function):
            result = result.filter(VOG.function.like("%" + d + "%"))

    if smin is not None:
        result = result.filter(VOG.species_count >= smin)
    if smax is not None:
        result = result.filter(VOG.species_count <= smax)

    if pmin is not None:
        result = result.filter(VOG.protein_count >= pmin)
    if pmax is not None:
        result = result.filter(VOG.protein_count <= pmax)

    if mingLCA is not None:
        result = result.filter(VOG.genomes_total_in_LCA >= mingLCA)
    if maxgLCA is not None:
        result = result.filter(VOG.genomes_total_in_LCA <= maxgLCA)
    
    if mingGLCA is not None:
        result = result.filter(VOG.genomes_in_group >= mingGLCA)
    if maxgGLCA is not None:
        result = result.filter(VOG.genomes_in_group <= maxgGLCA)

    if h_stringency is not None:
        result = result.filter(VOG.h_stringency == h_stringency)    
    if m_stringency is not None:
        result = result.filter(VOG.m_stringency == m_stringency)    
    if l_stringency is not None:
        result = result.filter(VOG.l_stringency == l_stringency)    
    if virus_specific is not None:
        result = result.filter(VOG.virus_specific == virus_specific)    

    if phages_nonphages:
        for d in set(phages_nonphages):
            result = result.filter(VOG.phages_nonphages.like("%" + d + "%"))

    if ancestors:
        for d in set(ancestors):
            result = result.filter(VOG.ancestors.like("%" + d + "%"))

    if proteins:
        for d in set(proteins):
            result = result.filter(VOG.proteins.any(Protein.id == d))

    if species:
        if union:
            sub = db.query(Member.vog_id).join(Protein).join(Species) \
                .filter(Species.species_name.in_(species)) \
                .subquery()
        else:
            sub = db.query(Member.vog_id).join(Protein).join(Species) \
                .filter(Species.species_name.in_(species)) \
                .group_by(Member.vog_id).having(func.count(Species.species_name) == len(species)) \
                .subquery()
        result = result.filter(VOG.id.in_(sub))

    if tax_id:
        ncbi = ncbi_taxa()
        try:
            if union:
                id_list = []
                for id in tax_id:
                    id_list.extend(
                        ncbi.get_descendant_taxa(id, collapse_subspecies=False, intermediate_nodes=True))
                    id_list.append(id)

                sub = db.query(Member.vog_id).join(Protein) \
                    .filter(Protein.taxon_id.in_(id_list)) \
                    .subquery()
                result = result.filter(VOG.id.in_(sub))
            else:
                for id in tax_id:
                    id_list = [id]
                    id_list.extend(
                        ncbi.get_descendant_taxa(id, collapse_subspecies=False, intermediate_nodes=True))

                    sub = db.query(Member.vog_id).join(Protein) \
                        .filter(Protein.taxon_id.in_(id_list)) \
                        .subquery()
                    result = result.filter(VOG.id.in_(sub))
        except ValueError:
            raise ValueError("The provided tax_id is invalid")

    return result.order_by(VOG.id).all()


def find_vogs_by_uid(db: Session, ids: Optional[List[str]]):
    """
    This function returns the VOG information based on the given VOG IDs
    """

    if ids:
        log.debug("Searching VOGs by IDs in the database...")

        return db.query(VOG).filter(VOG.id.in_(ids)).all()
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

    query = db.query(Protein.id)

    if taxon_id:
        query = query.filter(Protein.taxon_id.in_(set(taxon_id)))

    if vog_id:
        query = query.filter(Protein.vog_id.in_(set(vog_id)))

    if species:
        query = query.join(Species)
        for s in set(species):
            query = query.filter(Species.species_name.like("%" + s + "%"))

    return query.order_by(Protein.id).all()


def find_proteins_by_id(db: Session, pids: List[str]):
    """
    This function returns the Protein information based on the given Protein IDs
    """
    if pids:
        log.debug("Searching Proteins by ProteinIDs in the database...")

        return db.query(Protein).filter(Protein.id.in_(pids)).all()
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


def _load_gzipped_file_content(id: str, prefix: str, suffix: str) -> str:
    file_name = os.path.join(os.environ.get("VOG_DATA", "data"), prefix, id + suffix)
    with gzip.open(file_name, "rt") as f:
        return f.read()

