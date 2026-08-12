using System;
using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    internal readonly record struct CollectionRelation(string Name, int Oid);

    internal readonly record struct CollectionColumn(string Name, PgType PgType);

    internal static class CollectionCatalog
    {
        private const int FirstCollectionOid = 16384;

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

            // Oids aren't persisted; every catalog table must derive them the same way or joins silently return nothing.
            names.Sort(StringComparer.Ordinal);

            for (int i = 0; i < names.Count; i++)
                relations.Add(new CollectionRelation(names[i], FirstCollectionOid + i));

            return relations;
        }

        // Must match RqlQuery's RowDescription exactly, or PowerBI raises DataSource.Changed.
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

            yield return new CollectionColumn(PgSyntheticColumns.DocumentId, PgText.Default);

            var prop = default(BlittableJsonReaderObject.PropertyDetails);
            foreach (var name in sample.GetPropertyNames())
            {
                if (string.IsNullOrEmpty(name))
                    continue;
                if (name.StartsWith('@'))
                    continue;

                var propIdx = sample.GetPropertyIndex(name);
                if (propIdx == -1)
                    continue;
                sample.GetPropertyByIndex(propIdx, ref prop);

                yield return new CollectionColumn(name, MapPgType(prop.Token, prop.Value));
            }

            yield return new CollectionColumn(PgSyntheticColumns.Json, PgJson.Default);
        }

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
                _                      => "json",
            };
        }

        // Mirrors RqlQuery's BlittableJsonToken-to-PgType mapping.
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
                _                                    => PgJson.Default,
            };
        }
    }
}
