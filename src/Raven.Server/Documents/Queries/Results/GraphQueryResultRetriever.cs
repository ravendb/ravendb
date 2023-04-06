using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Corax;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using static Raven.Server.Documents.Queries.GraphQueryRunner;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Queries.Results
{
    public class GraphQueryResultRetriever : QueryResultRetrieverBase
    {
        private readonly GraphQuery _graphQuery;
        private readonly DocumentsOperationContext _context;

        public GraphQueryResultRetriever(GraphQuery graphQuery, DocumentDatabase database,
            IndexQueryServerSide query,
            QueryTimingsScope queryTimings,
            DocumentsStorage documentsStorage,
            DocumentsOperationContext context,
            FieldsToFetch fieldsToFetch,
            IncludeDocumentsCommand includeDocumentsCommand,
            IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand,
            IncludeRevisionsCommand includeRevisionsCommand)
            : base(database, query, queryTimings, SearchEngineType.Lucene, fieldsToFetch, documentsStorage, context, false, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand)
        {
            _graphQuery = graphQuery;
            _context = context;
        }

        protected override void ValidateFieldsToFetch(FieldsToFetch fieldsToFetch)
        {
            base.ValidateFieldsToFetch(fieldsToFetch);

            if (_query.ProjectionBehavior == null || _query.ProjectionBehavior == ProjectionBehavior.Default)
                return;

            throw new InvalidQueryException($"Invalid projection behavior '{_query.ProjectionBehavior}'.Only default projection behavior can be used for graph queries.", _query.Query, _query.QueryParameters);
        }

        public override (Document Document, List<Document> List) Get(ref RetrieverInput retrieverInput, CancellationToken token)
        {
            throw new NotSupportedException("Graph Queries do not deal with Lucene indexes.");
        }

        public override bool TryGetKeyLucene(ref RetrieverInput retrieverInput, out string key)
        {
            throw new NotSupportedException("Graph Queries do not deal with Lucene indexes.");
        }

        public override bool TryGetKeyCorax(IndexSearcher searcher, long id, out UnmanagedSpan key)
        {
            throw new NotSupportedException("Graph Queries do not deal with Corax indexes.");
        }

        public override Document DirectGet(ref RetrieverInput retrieverInput, string id, DocumentFields fields) => DocumentsStorage.Get(_context, id);

        protected override Document LoadDocument(string id) => DocumentsStorage.Get(_context, id);

        protected override long? GetCounter(string docId, string name)
        {
            throw new NotSupportedException("Graph Queries do not deal with Counters.");
        }

        protected override DynamicJsonValue GetCounterRaw(string docId, string name)
        {
            throw new NotSupportedException("Graph Queries do not deal with Counters.");
        }

        internal Document ProjectFromMatch(Match match, JsonOperationContext context, CancellationToken token)
        {
            var result = new DynamicJsonValue();
            result[Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Projection] = true
            };

            var item = new Document();
            foreach (var fieldToFetch in FieldsToFetch.Fields.Values)
            {
                object fieldVal;
                string key;
                if (fieldToFetch.QueryField?.ExpressionField?.Compound?.Contains("[]") ?? false)
                {
                    key = fieldToFetch.QueryField.ExpressionField.Compound.LastOrDefault().Value;
                }
                else
                {
                    key = fieldToFetch.ProjectedName ?? fieldToFetch.Name.Value;
                }

                if (fieldToFetch.QueryField.Function != null)
                {
                    var args = new object[fieldToFetch.FunctionArgs.Length + 1];
                    for (int i = 0; i < fieldToFetch.FunctionArgs.Length; i++)
                    {
                        var val = match.GetResult(fieldToFetch.FunctionArgs[i].ProjectedName);
                        switch (val)
                        {
                            case Document doc:
                                doc.EnsureMetadata();
                                args[i] = doc;
                                break;
                            case BlittableJsonReaderObject bjro:
                                args[i] = bjro;
                                break;
                            case List<Match> matchList:
                                CreateArgsFromMatchList(matchList, args, i);
                                break;
                            case MatchCollection matchCollection:
                                CreateArgsFromMatchList(matchCollection, args, i);
                                break;
                            case string s:
                                args[i] = s;
                                break;
                            default:
                                args[i] = null;
                                break;
                        }
                    }

                    key = fieldToFetch.ProjectedName ?? (fieldToFetch.ProjectedName ?? fieldToFetch.Name.Value);
                    fieldVal = GetFunctionValue(fieldToFetch, null, args, token);

                    var immediateResult = AddProjectionToResult(item, OneScore, FieldsToFetch, result, key, fieldVal);
                    if (immediateResult.Document != null)
                        return immediateResult.Document;
                }
                else
                {
                    var val = match.GetResult(fieldToFetch.QueryField.ExpressionField.Compound[0].Value);
                    var retriverInput = new RetrieverInput(null, null, null);
                    switch (val)
                    {
                        case Document d:
                            {
                                if (TryGetValue(fieldToFetch, d, ref retriverInput, null, null, out key, out fieldVal, token) == false)
                                    continue;
                                d.EnsureMetadata();
                                var immediateResult = AddProjectionToResult(d, OneScore, FieldsToFetch, result, key, fieldVal);
                                if (immediateResult.Document != null)
                                    return immediateResult.Document;
                                break;
                            }
                        case BlittableJsonReaderObject bjro:
                            {
                                var doc = new Document { Data = bjro };
                                if (TryGetValue(fieldToFetch, doc, ref retriverInput, null, null, out key, out fieldVal, token) == false)
                                    continue;
                                doc.EnsureMetadata();
                                var immediateResult = AddProjectionToResult(doc, OneScore, FieldsToFetch, result, key, fieldVal);
                                if (immediateResult.Document != null)
                                    return immediateResult.Document;
                                break;
                            }
                        case MatchCollection matches:
                            var array = new DynamicJsonArray();
                            foreach (var m in matches)
                            {
                                var djv = new DynamicJsonValue();
                                m.PopulateVertices(djv);

                                if (djv.Properties.Count == 0)
                                    continue;

                                var matchJson = _context.ReadObject(djv, "graph/arg");

                                var doc = new Document { Data = matchJson };

                                if (TryGetValue(fieldToFetch, doc, ref retriverInput, null, null, out key, out fieldVal, token) == false)
                                    continue;
                                doc.EnsureMetadata();
                                if (ReferenceEquals(doc, fieldVal))
                                    fieldVal = doc.Data;

                                array.Add(fieldVal);
                            }
                            result[key] = array;
                            break;
                        case string s:
                            result[fieldToFetch.ProjectedName ?? fieldToFetch.Name.Value] = s;
                            break;
                        default:
                            result[fieldToFetch.ProjectedName ?? fieldToFetch.Name.Value] = null;
                            continue;
                    }
                }
            }

            return new Document
            {
                Data = context.ReadObject(result, "projection result")
            };
        }

        private void CreateArgsFromMatchList(IEnumerable<Match> matchCollection, object[] args, int i)
        {
            var array = new DynamicJsonArray();
            foreach (var m in matchCollection)
            {
                var djv = new DynamicJsonValue();
                m.PopulateVertices(djv);
                array.Add(djv);
            }

            var dummy = new DynamicJsonValue();
            dummy["Dummy"] = array;
            args[i] = _context.ReadObject(dummy, "graph/arg")["Dummy"];
        }
    }
}
