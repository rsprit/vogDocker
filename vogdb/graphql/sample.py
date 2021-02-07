from vogdb.models import Species, Protein

from ariadne import gql, ObjectType, QueryType, fallback_resolvers, make_executable_schema
from ariadne.asgi import GraphQL

typedefs = gql("""

type Query {
    welcome: Welcome!

    species(ids: [ID!], names: [String!]): [Species!]

    proteins(ids: [ID!], names: [String!]): [Protein!]
}

type Welcome {
    message: String
    version: Int!
}

type Species {
    tax_id: ID!
    name:   String
    proteins: [Protein!]
    source: String
}

type Protein {
    id: ID!
    species: Species!
    vogs: [VOG!]
    aa_seq: String
    nt_seq: String
}

type VOG {
    id: ID!
    function: String
    consensus: String
    proteins: [Protein!]
}

""")

query = QueryType()

@query.field("welcome")
async def resolve_welcome(*_):
    return {
        'message': 'Welcome to VOGdb-API !',
        'version': 202
    }

@query.field("species")
async def resolve_species(_, info, ids=None, names=None):
    db = info.context["request"].state.db

    query = db.query(Species)

    if ids:
        query = query.filter(Species.taxon_id.in_(ids))

    if names:
        for name in names:
            query = query.filter(Species.species_name.like("%" + name + "%"))

    return query.order_by(Species.taxon_id).all()

@query.field("proteins")
async def resolve_proteins(_, info, ids=None, names=None):
    db = info.context["request"].state.db

    query = db.query(Protein)

    if ids:
        query = query.filter(Protein.id.in_(ids))

    if names:
        query = query.join(Species)
        for name in names:
            query = query.filter(Species.species_name.like("%" + name + "%"))

    return query.order_by(Protein.id).all()

species = ObjectType("Species")
species.set_alias("tax_id", "taxon_id")
species.set_alias("name", "species_name")

protein = ObjectType("Protein")

vog = ObjectType("VOG")
vog.set_alias("consensus", "consensus_function")

schema = make_executable_schema(typedefs, [query, vog, species, protein, fallback_resolvers])

app = GraphQL(schema, debug=True)