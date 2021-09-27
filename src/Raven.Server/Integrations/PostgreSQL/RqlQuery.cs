using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL
{
    public class RqlQuery : PgQuery
    {
        protected readonly DocumentDatabase DocumentDatabase;
        private List<Document> _result;
        private readonly int? _limit;

        public RqlQuery(string queryString, int[] parametersDataTypes, DocumentDatabase documentDatabase, int? limit = null) : base(queryString, parametersDataTypes)
        {
            DocumentDatabase = documentDatabase;
            _result = null;
            _limit = limit;
        }

        public override async Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false)
        {
            if (IsEmptyQuery)
                return default;

            await RunRqlQuery();
            return GenerateSchema();
        }

        public async Task RunRqlQuery()
        {
            var queryContext = QueryOperationContext.Allocate(DocumentDatabase);

            IndexQueryServerSide indexQuery;
            using (var jsonOperationContext = JsonOperationContext.ShortTermSingleUse())
            {
                var parameters = DynamicJsonValue.Convert(Parameters);
                indexQuery = new IndexQueryServerSide(QueryString,
                    jsonOperationContext.ReadObject(parameters, "query/parameters"));
            }

            var documentQueryResult = await DocumentDatabase.QueryRunner
                .ExecuteQuery(indexQuery, queryContext, null, OperationCancelToken.None);

            _result = documentQueryResult.Results;

            // If limit is 0, fetch one document for the schema generation
            if (_limit != null)
                _result = _result.Take(_limit.Value == 0 ? 1 : _limit.Value).ToList();

            // TODO: Support skipping (check how/if PowerBI sends it, probably using the incremental refresh feature)
            // query.Skip(..)
        }

        private ICollection<PgColumn> GenerateSchema()
        {
            if (_result == null || _result?.Count == 0)
                return Array.Empty<PgColumn>();

            var resultsFormat = GetDefaultResultsFormat();
            var sample = _result[0].Data;

            if (sample.TryGet("@metadata", out BlittableJsonReaderObject metadata)
                && metadata.TryGet("@id", out string _))
            {
                Columns["id()"] = new PgColumn("id()", (short)Columns.Count, PgText.Default, resultsFormat);
            }

            BlittableJsonReaderObject.PropertyDetails prop = default;

            // Go over sample's columns
            var properties = sample.GetPropertyNames();
            for (var i = 0; i < properties.Length; i++)
            {
                // Using GetPropertyIndex to get the properties in the right order
                var propIndex = sample.GetPropertyIndex(properties[i]);
                sample.GetPropertyByIndex(propIndex, ref prop);

                // Skip this column, will be added later to json() column
                if (prop.Name == "@metadata")
                    continue;

                PgType pgType = (prop.Token & BlittableJsonReaderBase.TypesMask) switch
                {
                    BlittableJsonToken.CompressedString => PgText.Default,
                    BlittableJsonToken.String => PgText.Default,
                    BlittableJsonToken.Boolean => PgBool.Default,
                    BlittableJsonToken.EmbeddedBlittable => PgJson.Default,
                    BlittableJsonToken.Integer => PgInt8.Default,
                    BlittableJsonToken.LazyNumber => PgFloat8.Default,
                    BlittableJsonToken.Null => PgJson.Default,
                    BlittableJsonToken.StartArray => PgJson.Default,
                    BlittableJsonToken.StartObject => PgJson.Default,
                    _ => throw new NotSupportedException()
                };

                var processedString = (prop.Token & BlittableJsonReaderBase.TypesMask) switch
                {
                    BlittableJsonToken.CompressedString => (string)prop.Value,
                    BlittableJsonToken.String => (LazyStringValue)prop.Value,
                    _ => null
                };

                if (processedString != null 
                    && TypeConverter.TryConvertStringValue(processedString, out var output))
                {
                    pgType = output switch
                    {
                        DateTime dt => (dt.Kind == DateTimeKind.Utc) ? PgTimestampTz.Default : PgTimestamp.Default,
                        DateTimeOffset => PgTimestampTz.Default,
                        TimeSpan => PgInterval.Default,
                        _ => pgType
                    };
                }

                Columns.TryAdd(prop.Name, new PgColumn(prop.Name, (short)Columns.Count, pgType, resultsFormat));
            }

            if (Columns.TryGetValue("json()", out var jsonColumn))
            {
                jsonColumn.PgType = PgJson.Default;
            }
            else
            {
                Columns["json()"] = new PgColumn("json()", (short)Columns.Count, PgJson.Default, resultsFormat);
            }

            return Columns.Values;
        }

        public static bool TryParse(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out RqlQuery rqlQuery)
        {
            // TODO: After integration - Use QueryParser to try and parse the query
            if (queryText.StartsWith("from", StringComparison.CurrentCultureIgnoreCase) ||
                queryText.StartsWith("/*rql*/", StringComparison.CurrentCultureIgnoreCase))
            {
                rqlQuery = new RqlQuery(queryText, parametersDataTypes, documentDatabase);
                return true;
            }

            rqlQuery = null;
            return false;
        }

        public override async Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token)
        {
            if (IsEmptyQuery)
            {
                await writer.WriteAsync(builder.EmptyQueryResponse(), token);
                return;
            }

            if (_result == null)
                throw new InvalidOperationException("RqlQuery.Execute was called when _results = null");

            if (_limit == 0 || _result == null || _result.Count == 0)
            {
                await writer.WriteAsync(builder.CommandComplete($"SELECT 0"), token);
                return;
            }

            BlittableJsonReaderObject.PropertyDetails prop = default;
            var row = ArrayPool<ReadOnlyMemory<byte>?>.Shared.Rent(Columns.Count);

            try
            {
                short? idIndex = null;
                if (Columns.TryGetValue("id()", out var col))
                {
                    idIndex = col.ColumnIndex;
                }

                var jsonIndex = Columns["json()"].ColumnIndex;

                for (int i = 0; i < _result.Count; i++)
                {
                    var result = _result[i].Data;
                    Array.Clear(row, 0, row.Length);

                    if (idIndex != null && 
                        result.TryGet("@metadata", out BlittableJsonReaderObject metadata) &&
                        metadata.TryGet("@id", out string id))
                    {
                        row[idIndex.Value] = Encoding.UTF8.GetBytes(id);
                    }

                    result.Modifications = new DynamicJsonValue(result);

                    foreach (var (key, pgColumn) in Columns)
                    {
                        var index = result.GetPropertyIndex(key);
                        if (index == -1)
                            continue;

                        result.GetPropertyByIndex(index, ref prop);

                        ReadOnlyMemory<byte>? value = null;
                        switch (prop.Token & BlittableJsonReaderBase.TypesMask, pgColumn.PgType.Oid)
                        {
                            case (BlittableJsonToken.Boolean, PgTypeOIDs.Bool):
                            case (BlittableJsonToken.CompressedString, PgTypeOIDs.Text):
                            case (BlittableJsonToken.EmbeddedBlittable, PgTypeOIDs.Json):
                            case (BlittableJsonToken.Integer, PgTypeOIDs.Int8):
                            case (BlittableJsonToken.String, PgTypeOIDs.Text):
                            case (BlittableJsonToken.StartArray, PgTypeOIDs.Json):
                            case (BlittableJsonToken.StartObject, PgTypeOIDs.Json):
                                value = pgColumn.PgType.ToBytes(prop.Value, pgColumn.FormatCode);
                                break;
                            case (BlittableJsonToken.LazyNumber, PgTypeOIDs.Float8):
                                value = pgColumn.PgType.ToBytes((double)(LazyNumberValue)prop.Value, pgColumn.FormatCode);
                                break;

                            case (BlittableJsonToken.CompressedString, PgTypeOIDs.Timestamp):
                            case (BlittableJsonToken.CompressedString, PgTypeOIDs.TimestampTz):
                            case (BlittableJsonToken.CompressedString, PgTypeOIDs.Interval):
                                {
                                    if (((string)prop.Value).Length != 0 && 
                                        TypeConverter.TryConvertStringValue((string)prop.Value, out var obj))
                                    {
                                        value = pgColumn.PgType.ToBytes(obj, pgColumn.FormatCode);
                                    }
                                    break;
                                }
                            case (BlittableJsonToken.String, PgTypeOIDs.Timestamp):
                            case (BlittableJsonToken.String, PgTypeOIDs.TimestampTz):
                            case (BlittableJsonToken.String, PgTypeOIDs.Interval):
                                {
                                    if (((LazyStringValue)prop.Value).Length != 0 && 
                                        TypeConverter.TryConvertStringValue((LazyStringValue)prop.Value, out object obj))
                                    {
                                        // TODO: Make pretty
                                        // Check for mismatch between column type and our data type
                                        if (obj is DateTime dt)
                                        {
                                            if (dt.Kind == DateTimeKind.Utc && pgColumn.PgType is not PgTimestampTz)
                                            {
                                                break;
                                            }
                                            else if (dt.Kind != DateTimeKind.Utc && pgColumn.PgType is not PgTimestamp)
                                            {
                                                break;
                                            }
                                        }

                                        if (obj is DateTimeOffset && pgColumn.PgType is not PgTimestampTz)
                                        {
                                            break;
                                        }

                                        if (obj is TimeSpan && pgColumn.PgType is not PgInterval)
                                        {
                                            break;
                                        }

                                        value = pgColumn.PgType.ToBytes(obj, pgColumn.FormatCode);
                                    }
                                    break;
                                }
                            case (BlittableJsonToken.String, PgTypeOIDs.Float8):
                                value = pgColumn.PgType.ToBytes(double.Parse((LazyStringValue)prop.Value), pgColumn.FormatCode);
                                break;
                            case (BlittableJsonToken.Null, PgTypeOIDs.Json):
                                value = Array.Empty<byte>();
                                break;
                        }

                        if (value == null)
                        {
                            continue;
                        }
                        row[pgColumn.ColumnIndex] = value;
                        result.Modifications.Remove(key);
                    }

                    if (result.Modifications.Removals.Count != result.Count)
                    {
                        using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            BlittableJsonReaderObject modified;

                            modified = context.ReadObject(result, "renew");
                            row[jsonIndex] = Encoding.UTF8.GetBytes(modified.ToString());
                        }
                    }

                    await writer.WriteAsync(builder.DataRow(row[..Columns.Count]), token);
                }
            }
            finally
            {
                ArrayPool<ReadOnlyMemory<byte>?>.Shared.Return(row);
            }

            await writer.WriteAsync(builder.CommandComplete($"SELECT {_result.Count}"), token);
        }

        public override void Dispose()
        {
            //_session?.Dispose();
        }
    }
}
