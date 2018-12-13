using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

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
            IncludeDocumentsCommand includeDocumentsCommand)
            : base(database, query, queryTimings, fieldsToFetch, documentsStorage, context, false, includeDocumentsCommand)
        {
            _graphQuery = graphQuery;
            _context = context;
        }

        public Document Get(Document doc)
        {
            return GetProjectionFromDocument(doc, null, 0, FieldsToFetch, _context, null);
        }      

        public override Document Get(Lucene.Net.Documents.Document input, float score, IState state)
        {
            throw new NotSupportedException("Graph Queries do not deal with Lucene indexes.");
        }

        public override bool TryGetKey(Lucene.Net.Documents.Document document, IState state, out string key)
        {
            throw new NotSupportedException("Graph Queries do not deal with Lucene indexes.");
        }

        protected override Document DirectGet(Lucene.Net.Documents.Document input, string id, IState state) => 
            DocumentsStorage.Get(_context, id);

        protected override Document LoadDocument(string id) => DocumentsStorage.Get(_context, id);

        protected override long? GetCounter(string docId, string name)
        {
            throw new NotSupportedException("Graph Queries do not deal with Counters.");
        }

        protected override DynamicJsonValue GetCounterRaw(string docId, string name)
        {
            throw new NotSupportedException("Graph Queries do not deal with Counters.");
        }

        internal Document ProjectFromMatch(Match match, JsonOperationContext context)
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
                string key = fieldToFetch.ProjectedName ?? fieldToFetch.Name.Value;
                if (fieldToFetch.QueryField.Function != null)
                {
                    var args = new object[fieldToFetch.FunctionArgs.Length + 1];
                    for (int i = 0; i < fieldToFetch.FunctionArgs.Length; i++)
                    {
                        var val = match.GetResult(fieldToFetch.FunctionArgs[i].ProjectedName);
                        switch (val)
                        {
                            case Document doc:
                                args[i] = doc;
                                break;
                            case BlittableJsonReaderObject bjro:
                                args[i] = bjro;
                                break;
                            case List<Match> matches:
                                var array = new DynamicJsonArray();
                                foreach (var m in matches)
                                {
                                    var djv = new DynamicJsonValue();
                                    m.PopulateVertices(djv);
                                    array.Add(djv);
                                }

                                var dummy = new DynamicJsonValue();
                                dummy["Dummy"] = array;
                                args[i] = _context.ReadObject(dummy, "graph/arg")["Dummy"];
                                break;
                            case string s:
                                args[i] = s;
                                break;
                            default:
                                args[i] = null;
                                break;
                        }
                    }

                    key = fieldToFetch.ProjectedName;
                    fieldVal = GetFunctionValue(fieldToFetch, args);

                    var immediateResult = AddProjectionToResult(item, 1f, FieldsToFetch, result, key, fieldVal);
                    if (immediateResult != null)
                        return immediateResult;
                }
                else
                {
                    var val = match.GetResult(fieldToFetch.QueryField.ExpressionField.Compound[0].Value);
                    switch (val)
                    {
                        case Document doc:
                        {
                            doc.EnsureMetadata();
                            if (TryGetValue(fieldToFetch, doc, null, null, out key, out fieldVal) == false)
                                continue;

                            var immediateResult = AddProjectionToResult(doc, 1f, FieldsToFetch, result, key, fieldVal);
                            if (immediateResult != null)
                                return immediateResult;
                            break;
                        }
                        case BlittableJsonReaderObject bjro:
                        {
                            var doc = new Document { Data = bjro };
                            doc.EnsureMetadata();

                            if (TryGetValue(fieldToFetch, doc, null, null, out key, out fieldVal) == false)
                                continue;
                       
                            var immediateResult = AddProjectionToResult(d, 1f, FieldsToFetch, result, key, fieldVal);
                            if (immediateResult != null)
                                return immediateResult;
                            break;
                        }
                        case BlittableJsonReaderObject bjro:
                        {
                            var doc = new Document { Data = bjro };
                            if (TryGetValue(fieldToFetch, doc, null, null, out key, out fieldVal) == false)
                                continue;

                            var immediateResult = AddProjectionToResult(doc, 1f, FieldsToFetch, result, key, fieldVal);
                            if (immediateResult != null)
                                return immediateResult;
                            break;
                        }
                        case List<Match> matches:
                            var array = new DynamicJsonArray();
                            foreach (var m in matches)
                            {
                                var djv = new DynamicJsonValue();
                                m.PopulateVertices(djv);

                                if (djv.Properties.Count == 0)
                                    continue;

                                var matchJson = _context.ReadObject(djv, "graph/arg");

                                var doc = new Document { Data = matchJson };
                                doc.EnsureMetadata();
                                if (TryGetValue(fieldToFetch, doc, null, null, out key, out fieldVal) == false)
                                    continue;

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
                            break;
                    }
                }

            }

            return new Document
            {
                Data = context.ReadObject(result, "projection result")
            };
        }
    }
}
