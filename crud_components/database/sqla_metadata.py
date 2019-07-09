import sqlalchemy as sa


metadata = sa.MetaData(
    # schema='medicall'  # It does not set the schema on types (enums)
    # naming_convention=...
)
