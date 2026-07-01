using Npgsql.Replication.PgOutput.Messages;

namespace Raven.Server.Documents.CdcSink.Schema
{
    /// <summary>
    /// Maps PostgreSQL type OIDs into the CDC type-category enum used by both the
    /// streaming/initial-load path (via <see cref="PostgresCdcSinkProcess"/>) and the
    /// schema-discovery endpoint. Centralising the OID list here keeps the two callers
    /// from drifting — what CDC accepts at runtime is exactly what the schema endpoint
    /// reports as capturable.
    /// </summary>
    internal static class PostgresColumnTypeMapping
    {
        /// <summary>
        /// Look up a Postgres OID. <paramref name="vectorOid"/> is the OID of the
        /// pgvector extension if installed, <see cref="uint.MaxValue"/> otherwise — it
        /// varies per database so it has to be resolved at runtime and passed in.
        /// </summary>
        public static PostgresTypeCategory OidToCategory(uint oid, uint vectorOid)
        {
            return oid switch
            {
                21 or 23 or 26 => PostgresTypeCategory.Integer,    // int2, int4, oid
                20 => PostgresTypeCategory.BigInt,                  // int8
                700 => PostgresTypeCategory.Float,                  // float4
                701 => PostgresTypeCategory.Double,                 // float8
                1700 => PostgresTypeCategory.Numeric,               // numeric/decimal
                16 => PostgresTypeCategory.Boolean,                 // bool
                1082 => PostgresTypeCategory.DateOnly,              // date
                1114 or 1184 => PostgresTypeCategory.DateTime,      // timestamp, timestamptz
                2950 => PostgresTypeCategory.Uuid,                  // uuid
                17 => PostgresTypeCategory.Bytea,                   // bytea
                114 or 3802 => PostgresTypeCategory.Json,           // json, jsonb
                // Array types — Postgres has a dedicated OID for each base type's array form.
                // pgoutput delivers these as text literals like "{tag1,tag2,tag3}".
                1000 or 1001 or 1005 or 1007 or 1009 or 1015 or 1016
                    or 1021 or 1022 or 1028 or 1231 or 2951 or 199 or 3807
                    => PostgresTypeCategory.TextArray,              // bool[], bytea[], int2[], int4[], text[], varchar[], int8[], float4[], float8[], oid[], numeric[], uuid[], json[], jsonb[]
                _ when oid == vectorOid => PostgresTypeCategory.Vector,
                _ => PostgresTypeCategory.Other,
            };
        }

        public static PostgresTypeCategory[] BuildTypeCategoriesFromRelation(RelationMessage relation, uint vectorOid)
        {
            var categories = new PostgresTypeCategory[relation.Columns.Count];
            for (int i = 0; i < relation.Columns.Count; i++)
            {
                categories[i] = OidToCategory(relation.Columns[i].DataTypeId, vectorOid);
            }
            return categories;
        }
    }

    internal enum PostgresTypeCategory
    {
        Other,
        Integer,
        BigInt,
        Float,
        Double,
        Numeric,
        Boolean,
        DateOnly,
        DateTime,
        Uuid,
        Bytea,
        Json,
        TextArray,
        Vector
    }
}
