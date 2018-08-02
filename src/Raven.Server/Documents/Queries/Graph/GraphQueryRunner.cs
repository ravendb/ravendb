using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Extensions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryRunner
    {
        protected DocumentDatabase Database;
        private readonly GraphQuery _q;

        public GraphQueryRunner([NotNull] DocumentDatabase database, [NotNull] GraphQuery q)
        {
            Database = database ?? throw new ArgumentNullException(nameof(database));
            _q = q ?? throw new ArgumentNullException(nameof(q));
        }

        public async Task<DocumentQueryResult> RunAsync()
        {
            var withQueryRunner = new DynamicQueryRunner(Database);
            //first do the with clause queries
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var withClauseResults = new Dictionary<string, List<Match>>();
                foreach (var withClause in _q.WithDocumentQueries)
                {
                    var withClauseQueryResult =
                        await withQueryRunner.ExecuteQuery(new IndexQueryServerSide(withClause.Value.ToString()), ctx, null, OperationCancelToken.None);
                    var queryMatches = new List<Match>();
                    foreach (var result in withClauseQueryResult.Results)
                    {
                        var match = new Match();
                        match.Set(withClause.Key, result.Data);
                        queryMatches.Add(match);
                    }
                    withClauseResults.Add(withClause.Key, queryMatches);
                }

                Execute(withClauseResults, _q.MatchClause);



                return new DocumentQueryResult
                {
                };
            }
        }

        private struct Match
        {
            private Dictionary<string, BlittableJsonReaderObject> _inner;


            public BlittableJsonReaderObject Get(string alias)
            {
                BlittableJsonReaderObject result = null;
                _inner?.TryGetValue(alias, out result);
                return result;
            }

            public void Set(string alias, BlittableJsonReaderObject val)
            {
                if (_inner == null)
                    _inner = new Dictionary<string, BlittableJsonReaderObject>();

                _inner.Add(alias, val);
            }
        }

        private List<Match> Execute(Dictionary<string, List<Match>> source, PatternMatchExpression matchClause)
        {
            switch (matchClause)
            {
                case PatternMatchBinaryExpression patternMatchBinaryExpression:
                    var left = Execute(source, patternMatchBinaryExpression.Left);
                    var right = Execute(source, patternMatchBinaryExpression.Right);

                    switch (patternMatchBinaryExpression.Op)
                    {
                        case PatternMatchBinaryExpression.Operator.And:
                            return left.Intersect(right).ToList();
                        case PatternMatchBinaryExpression.Operator.Or:
                            return left.Union(right).ToList();
                        case PatternMatchBinaryExpression.Operator.AndNot:
                            return left.Except(right).ToList();
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                case PatternMatchElementExpression patternMatchElementExpression:
                    return ExecuteGraphPattern(patternMatchElementExpression, source);
                default:
                    throw new ArgumentException("Unknown type " + matchClause);
            }
        }

        private List<Match> ExecuteGraphPattern(PatternMatchElementExpression p, Dictionary<string, List<Match>> source)
        {
            if (!source.TryGetValue(p.FromAlias, out var start) || !source.TryGetValue(p.ToAlias, out var end))
            {
                return new List<Match>();
            }

            var final = new List<Match>();
            foreach (var v in start)
            {
                var edges = GetEdges(v.Get(p.FromAlias), p.EdgeAlias);
                foreach (var edge in edges)
                {
                    if (edge.TryGet("@to", out string to) == false)
                        continue;
                    var d = end.Find(x => x.Get(p.ToAlias)?.GetMetadata()?.GetId() == to);

                    var match = new Match();
                    match.Set(p.FromAlias, v.Get(p.FromAlias));
                    match.Set(p.ToAlias, d.Get(p.ToAlias));
                    if (p.EdgeAlias != null)
                        match.Set(p.EdgeAlias, edge);

                    final.Add(match);
                }
            }

            return final;

        }

        private IEnumerable<BlittableJsonReaderObject> GetEdges(BlittableJsonReaderObject doc, string edgeAlias)
        {
            if (doc.TryGet("@metadata", out BlittableJsonReaderObject metadata) == false)
                return Array.Empty<BlittableJsonReaderObject>();

            if (doc.TryGet("@edges", out BlittableJsonReaderObject edges) == false)
                return Array.Empty<BlittableJsonReaderObject>();


            WithEdgesExpression edgesExpression = null;
            if (edgeAlias != null && _q.WithEdgePredicates.TryGetValue(edgeAlias, out edgesExpression) == false)
            {
                throw new InvalidOperationException("Missing edge alias criteria: " + edgeAlias);
            }

            if (edgesExpression == null)
            {
                return GetAllEdges(doc, edges);
            }

            // TODO: actually implement filter on edges, order by, etc
            return GetTypedEdges(doc, edges, edgesExpression.EdgeType);
        }

        private IEnumerable<BlittableJsonReaderObject> GetAllEdges(BlittableJsonReaderObject doc, BlittableJsonReaderObject edges)
        {
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < edges.Count; i++)
            {
                edges.GetPropertyByIndex(i, ref prop);
                var actualEdges = (BlittableJsonReaderArray)prop.Value;
                foreach (BlittableJsonReaderObject t in actualEdges)
                {
                    yield return t;
                }
            }
        }

        /*
         * @metadata: { @edges: { Rated: [{To: "movies/1", Rating: 4.5 }]
         */

        private IEnumerable<BlittableJsonReaderObject> GetTypedEdges(BlittableJsonReaderObject doc, BlittableJsonReaderObject edges,
            string type)
        {
            if (edges.TryGet(type, out BlittableJsonReaderArray actualEdges) == false)
                yield break;
            foreach (BlittableJsonReaderObject t in actualEdges)
            {
                yield return t;
            }
        }
    }
}

