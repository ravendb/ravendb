using System;
using System.Collections.Generic;
using Lucene.Net.Store;
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

        internal Document ProjectFromMatch(GraphQueryRunner.Match match, JsonOperationContext context)
        {
            var result = new DynamicJsonValue();
            result[Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Raven.Client.Constants.Documents.Metadata.Projection] = true
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
                        if(val is Document d)
                        {
                            args[i] = d;
                        }
                        else if (val is BlittableJsonReaderObject bjro)
                        {
                            args[i] = bjro;
                        }
                        else if( val is List<Match> matches)
                        {
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
                        }
                        else if(val is string s)
                        {
                            args[i] = s;
                        }
                        else
                        {
                            args[i] = null;
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
                    var val = match.GetResult(fieldToFetch.QueryField.ExpressionField.Compound[0]);
                    if (val is Document d)
                    {
                        if (TryGetValue(fieldToFetch, d, null, null, out key, out fieldVal) == false)
                            continue;

                        var immediateResult = AddProjectionToResult(d, 1f, FieldsToFetch, result, key, fieldVal);
                        if (immediateResult != null)
                            return immediateResult;
                    }
                    else if (val is List<Match> matches)
                    {
                        var array = new DynamicJsonArray();
                        foreach (var m in matches)
                        {
                            var djv = new DynamicJsonValue();
                            m.PopulateVertices(djv);

                            if (djv.Properties.Count == 0)
                                continue;

                            var matchJson = _context.ReadObject(djv, "graph/arg");

                            if (TryGetValue(fieldToFetch, new Document { Data = matchJson }, null, null, out key, out fieldVal) == false)
                                continue;

                            array.Add(fieldVal);
                        }
                        result[key] = array;
                    }
                    else if (val is string s)
                    {
                        result[fieldToFetch.ProjectedName ?? fieldToFetch.Name.Value] = s;
                    }
                    else
                    {
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
    }
}
