using System;
using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    // A collection as a relation: its name, and the oid every other catalog row keys on.
    internal readonly record struct CollectionRelation(string Name, int Oid);

    // One reflected column of a collection: the name a client sees and the PG type RqlQuery will
    // send it as. information_schema.columns spells the type out (data_type); pg_attribute reports
    // the same type as an oid (atttypid).
    internal readonly record struct CollectionColumn(string Name, PgType PgType);

    // Everything the catalog tables report about the database, derived in one place: which
    // relations exist (one per collection) and what columns each of them has.
    //
    // Two catalogs expose this and a client may reflect through either - information_schema
    // (.tables / .columns) and pg_catalog (pg_class / pg_attribute). They must not disagree:
    // pg_class hands out an oid that pg_attribute's rows are keyed by, so a client that reads the
    // oid from one query and joins on it in the next gets nothing at all if the two derive it
    // differently. Sourcing both from here is what keeps them in step.
    internal static class CollectionCatalog
    {
        // PG's first non-system oid: collections are user relations, and clients (pgAdmin) filter
        // system objects out with `oid > 16383`. pg_database's single row uses this same value -
        // harmless, since an oid is only ever compared against others from the same catalog.
        private const int FirstCollectionOid = 16384;

        // One relation per collection, in oid order.
        public static List<CollectionRelation> Relations(VirtualQueryContext ctx)
        {
            var relations = new List<CollectionRelation>();
            if (ctx?.Database == null)
                return relations;

            var names = new List<string>();

            using (ctx.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var collection in ctx.Database.DocumentsStorage.GetCollections(context))
                {
                    if (CollectionName.IsHiLoCollection(collection.Name))
                        continue;

                    names.Add(collection.Name);
                }
            }

            // Nothing persists these oids, so they're derived: name order from a fixed base means
            // the same set of collections always yields the same oid for the same name, which is
            // what a client that reads oids from one query and uses them in the next relies on.
            names.Sort(StringComparer.Ordinal);

            for (int i = 0; i < names.Count; i++)
                relations.Add(new CollectionRelation(names[i], FirstCollectionOid + i));

            return relations;
        }

        // The columns of one collection, in the order a client will see them.
        //
        // The reported columns MUST match what RqlQuery emits in its RowDescription (same count,
        // order, names, and types) or PowerBI raises DataSource.Changed. Hence: user columns in
        // document insertion order (GetPropertyNames, not GetPropertyByIndex), bracketed by the
        // synthetic id/json columns, with types mirroring RqlQuery's mapping (see MapPgType).
        //
        // RavenDB has no schema, so the first document in the collection is the only column shape
        // there is to report; an empty collection yields no columns.
        public static IEnumerable<CollectionColumn> Columns(DocumentDatabase database, DocumentsOperationContext context, string collection)
        {
            BlittableJsonReaderObject sample = null;
            foreach (var doc in database.DocumentsStorage.GetDocumentsFrom(context, collection, etag: 0, start: 0, take: 1))
            {
                sample = doc.Data;
                break;
            }

            if (sample == null)
                yield break;

            // 1) Synthetic id column (PG-facing name; see PgSyntheticColumns).
            yield return new CollectionColumn(PgSyntheticColumns.DocumentId, PgText.Default);

            // 2) User columns in document insertion order (same order RqlQuery emits).
            var prop = default(BlittableJsonReaderObject.PropertyDetails);
            foreach (var name in sample.GetPropertyNames())
            {
                if (string.IsNullOrEmpty(name))
                    continue;
                // Skip RavenDB system fields (@metadata, etc.); RqlQuery skips them too.
                if (name.StartsWith('@'))
                    continue;

                var propIdx = sample.GetPropertyIndex(name);
                if (propIdx == -1)
                    continue;
                sample.GetPropertyByIndex(propIdx, ref prop);

                yield return new CollectionColumn(name, MapPgType(prop.Token, prop.Value));
            }

            // 3) json - the metadata blob column RqlQuery appends last (PgJson).
            yield return new CollectionColumn(PgSyntheticColumns.Json, PgJson.Default);
        }

        // information_schema.columns.data_type: the SQL name of the type pg_attribute reports as an
        // oid. Only the types MapPgType can produce appear here - one mapping rendered two ways, so
        // the two catalogs can't drift.
        public static string DataTypeName(PgType type)
        {
            return type.Oid switch
            {
                PgTypeOIDs.Text        => "text",
                PgTypeOIDs.Int8        => "bigint",
                PgTypeOIDs.Float8      => "double precision",
                PgTypeOIDs.Bool        => "boolean",
                PgTypeOIDs.Timestamp   => "timestamp without time zone",
                PgTypeOIDs.TimestampTz => "timestamp with time zone",
                PgTypeOIDs.Interval    => "interval",
                _                      => "json",   // PgJson, and the unknown -> PgJson fallback
            };
        }

        // Mirrors RqlQuery's BlittableJsonToken-to-PgType mapping so a reflected column reports the
        // type RqlQuery will actually send (see the class doc on why that must hold). For
        // String/CompressedString, peek at the value like RqlQuery does: datetime-shaped strings
        // map to timestamp, not text.
        private static PgType MapPgType(BlittableJsonToken token, object value)
        {
            var bjt = token & BlittableJsonToken.TypesMask;

            if (bjt is BlittableJsonToken.String or BlittableJsonToken.CompressedString)
            {
                var processedString = bjt == BlittableJsonToken.CompressedString
                    ? (string)(LazyCompressedStringValue)value
                    : (string)(LazyStringValue)value;

                if (processedString != null
                    && TypeConverter.TryConvertStringValue(processedString, out var parsed))
                {
                    return parsed switch
                    {
                        DateTime dt      => dt.Kind == DateTimeKind.Utc
                                                ? PgTimestampTz.Default
                                                : (PgType)PgTimestamp.Default,
                        DateTimeOffset   => PgTimestampTz.Default,
                        TimeSpan         => PgInterval.Default,
                        _                => PgText.Default
                    };
                }

                return PgText.Default;
            }

            return bjt switch
            {
                BlittableJsonToken.Integer           => PgInt8.Default,
                BlittableJsonToken.LazyNumber        => PgFloat8.Default,
                BlittableJsonToken.Boolean           => PgBool.Default,
                BlittableJsonToken.StartObject       => PgJson.Default,
                BlittableJsonToken.StartArray        => PgJson.Default,
                BlittableJsonToken.EmbeddedBlittable => PgJson.Default,
                BlittableJsonToken.Null              => PgJson.Default,
                _                                    => PgJson.Default,   // unknown -> PgJson
            };
        }
    }
}
