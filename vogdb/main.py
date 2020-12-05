import sys
from fastapi import Query, Path, HTTPException
from typing import Optional, Set, List
from .functionality import VogService, find_vogs_by_uid, get_proteins, get_vogs
from .database import SessionLocal
from sqlalchemy.orm import Session
from fastapi import Depends, FastAPI


from .schemas import VOG_profile, Protein_profile, Filter, VOG_UID
from . import models

api = FastAPI()
svc = VogService('data')


# Dependency. Connect to the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@api.get("/")
async def root():
    return {"message": "Welcome to VOGDB-API"}


@api.get("/vsearch/vog/",
         response_model=List[VOG_UID])
def search_vog(db: Session = Depends(get_db),
               id: Optional[Set[str]] = Query(None),
               pmin: Optional[int] = None,
               pmax: Optional[int] = None,
               smax: Optional[int] = None,
               smin: Optional[int] = None,
               functional_category: Optional[Set[str]] = Query(None),
               consensus_function: Optional[Set[str]] = Query(None),
               mingLCA: Optional[int] = None,
               maxgLCA: Optional[int] = None,
               mingGLCA: Optional[int] = None,
               maxgGLCA: Optional[int] = None,
               ancestors: Optional[Set[str]] = Query(None),
               h_stringency: Optional[bool] = None,
               m_stringency: Optional[bool] = None,
               l_stringency: Optional[bool] = None,
               virus_specific: Optional[bool] = None,
               proteins: Optional[Set[str]] = Query(None),
               species: Optional[Set[str]] = Query(None),
               ):
    """
    This functions searches a database and returns a list of vog unique identifiers (UIDs) for records in that database
    which meet the search criteria.
    :return:
    """

    vogs = get_vogs(db, models.VOG_profile.id , id, pmin, pmax, smax, smin, functional_category, consensus_function, mingLCA, maxgLCA, mingGLCA, maxgGLCA,
                   ancestors, h_stringency, m_stringency, l_stringency, virus_specific, proteins, species)

    if not vogs:
        raise HTTPException(status_code=404, detail="No VOGs match the search criteria.")
    return vogs


@api.get("/vsummary/vog/",
         response_model=List[VOG_profile])
async def get_summary(uid: List[str] = Query(None), db: Session = Depends(get_db)):
    """
    This function returns vog summaries for a list of unique identifiers (UIDs)
    :param uid: VOGID
    :param db: database session dependency
    :return: vog summary
    """

    vog_summary = find_vogs_by_uid(db, uid)

    if not vog_summary:
        raise HTTPException(status_code=404, detail="No matching VOGs found")

    return vog_summary

@api.get("/vfetch/vog/")
async def fetch_vog(uid: List[str] = Query(None), db: Session = Depends(get_db)):
    """
    This function returns vog data for a list of unique identifiers (UIDs)
    :param uid: VOGID
    :param db: database session dependency
    :return: vog data (HMM profile, MSE...)
    """

    #ToDo: implement...

    return 0


#ToDo: implement the same idea as above, for species and proteins...
@api.get("/vsearch/species/")
def search_species():
    return "No yet implemented"


@api.get("/vsearch/protein/")
def search_protein():
    return "No yet implemented"






# OLD
#
# @api.get("/vog_profile1/", response_model=List[VOG_profile])
# def read_vog(ids: Optional[List[str]] = Query(None), db: Session = Depends(get_db)):
#     """This function takes a list of VOGids and returns all the matching VOG_profiles
#     """
#     vogs = find_vogs_by_uid(db, ids)
#
#     if not vogs:
#         raise HTTPException(status_code=404, detail="No VOGs found")
#     return vogs
#
#
# # VOG FILTERING:
# @api.get("/vog_filter/", response_model=List[VOG_profile])
# def vog_filter(db: Session = Depends(get_db), names: Optional[Set[str]] = Query(None),
#                fct_description: Optional[Set[str]] = Query(None),
#                fct_category: Optional[Set[str]] = Query(None), gmin: Optional[int] = None, gmax: Optional[int] = None,
#                pmin: Optional[int] = None, pmax: Optional[int] = None, species: Optional[Set[str]] = Query(None),
#                protein_names: Optional[Set[str]] = Query(None), mingLCA: Optional[int] = None,
#                maxgLCA: Optional[int] = None,
#                mingGLCA: Optional[int] = None, maxgGLCA: Optional[int] = None,
#                ancestors: Optional[Set[str]] = Query(None),
#                h_stringency: Optional[bool] = None, m_stringency: Optional[bool] = None,
#                l_stringency: Optional[bool] = None,
#                virus_spec: Optional[bool] = None):
#     vogs = vog_get(db, names, fct_description, fct_category, gmin, gmax, pmin, pmax, species, protein_names, mingLCA, maxgLCA, mingGLCA, maxgGLCA,
#                       ancestors, h_stringency, m_stringency, l_stringency, virus_spec)
#     if not vogs:
#         raise HTTPException(status_code=404, detail="No VOGs found")
#     return vogs
#
#
#
# @api.get("/protein_profile1/", response_model=List[Protein_profile])
# def read_protein(species: str = Query(None), db: Session = Depends(get_db)):
#     """This function takes only one species and returns all protein profiles associated with this species/family
#     """
#
#     proteins = get_proteins(db, species)
#     if not proteins:
#         raise HTTPException(status_code=404, detail="User not found")
#     return proteins
#
#
# @api.get("/species")
# async def get_species(name: Optional[Set[str]] = Query(None), id: Optional[Set[int]] = Query(None),
#                       phage: Optional[bool] = None, source: Optional[str] = None):
#     response = list(svc.species.search(name=name, ids=id, phage=phage, source=source))
#     if not response:
#         return {"message": "Nothing could be found for your search options."}
#     return response
#
#
# @api.get("/vog")
# async def get_vogs(
#         names: Optional[Set[str]] = Query(None), fct_description: Optional[Set[str]] = Query(None),
#         fct_category: Optional[Set[str]] = Query(None), gmin: Optional[int] = None, gmax: Optional[int] = None,
#         pmin: Optional[int] = None, pmax: Optional[int] = None, species: Optional[Set[str]] = Query(None),
#         protein_names: Optional[Set[str]] = Query(None), mingLCA: Optional[int] = None, maxgLCA: Optional[int] = None,
#         mingGLCA: Optional[int] = None, maxgGLCA: Optional[int] = None, ancestors: Optional[Set[str]] = Query(None),
#         h_stringency: Optional[bool] = None, m_stringency: Optional[bool] = None, l_stringency: Optional[bool] = None,
#         virus_spec: Optional[bool] = None):
#     response = list(svc.groups.search(names=names, fct_description=fct_description, fct_category=fct_category,
#                                       gmin=gmin, gmax=gmax, pmin=pmin, pmax=pmax, species=species,
#                                       protein_names=protein_names, mingLCA=mingLCA, maxgLCA=maxgLCA, mingGLCA=mingGLCA,
#                                       maxgGLCA=maxgGLCA, ancestors=ancestors, h_stringency=h_stringency,
#                                       m_stringency=m_stringency, l_stringency=l_stringency, virus_spec=virus_spec))
#     if not response:
#         return {"message": "Nothing could be found for your search options."}
#     return response
#
#
# @api.post("/filter/")
# async def create_filter(paras: Filter):
#     print("filter")
#     #
#     # finds aufrufen...
#     return paras
#
#
# searches = {
#     "search1": {"sid": 1002724},
#     "search2": {"sn": "India", "phage": True}
# }

# @api.get("/vog_filtering/{Filter: paras}", response_model=List[VOG])


# @api.get("/species_filtering/", response_model=List[Species])

# @api.get("/protein_filtering/", response_model=List[Protein])