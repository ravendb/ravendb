using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryPlan
    {
        private IGraphQueryStep _rootQueryStep;
        private IndexQueryServerSide _query;
        private readonly DocumentsOperationContext _context;
        private readonly long? _resultEtag;
        private readonly OperationCancelToken _token;
        private readonly DocumentDatabase _database;
        public GraphQuery GraphQuery => _query.Metadata.Query.GraphQuery;

        public GraphQueryPlan(IndexQueryServerSide query, DocumentsOperationContext context, long? resultEtag,
            OperationCancelToken token, DocumentDatabase database)
        {
            _database = database;
            _query = query;
            _context = context;
            _resultEtag = resultEtag;
            _token = token;
        }


        public void BuildQueryPlan()
        {
            _rootQueryStep = BuildQueryPlanForExpression(_query.Metadata.Query.GraphQuery.MatchClause);                       
        }

        private IGraphQueryStep BuildQueryPlanForExpression(QueryExpression expression)
        {
            switch (expression)
            {
                case PatternMatchElementExpression pme:
                    return BuildQueryPlanForPattern(pme);
                case BinaryExpression be:
                    return BuildQueryPlanForBinaryExpression(be);
                default:
                    throw new ArgumentOutOfRangeException($"Unexpected expression of type {expression.Type}");
            }
        }

        public (Dictionary<string, object> Nodes, Dictionary<object, HashSet<string>> Edges) Analyze(List<Match> matches)
        {
            var edges = new Dictionary<object, HashSet<string>>();
            var nodes = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);



            foreach (var match in matches)
            {
                _rootQueryStep.Analyze(match, AddNode, AddEdge);
            }

            return (nodes, edges);

            void AddEdge(object src, string dst)
            {
                if (src == null || dst == null)
                    return;

                if (edges.TryGetValue(src, out var hash) == false)
                    edges[src] = hash = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                hash.Add(dst);
            }

            void AddNode(string key, object val)
            {
                key = key ?? "__anonymous__/" + Guid.NewGuid();
                nodes[key] = val;
            }
        }

        public async Task Initialize()
        {
            await _rootQueryStep.Initialize();
        }

        private IGraphQueryStep BuildQueryPlanForBinaryExpression(BinaryExpression be)
        {
            bool negated = false;
            var rightExpr = be.Right;
            if(be.Right is NegatedExpression n)
            {
                negated = true;
                rightExpr = n.Expression;
            }

            var left = BuildQueryPlanForExpression(be.Left);
            var right = BuildQueryPlanForExpression(rightExpr);
            switch (be.Operator)
            {
                case OperatorType.And:
                    if(negated)
                        return new IntersectionQueryStep<Except>(left, right);

                    return new IntersectionQueryStep<Intersection>(left, right);

               case OperatorType.Or:
                    return new IntersectionQueryStep<Union>(left, right);

                default:
                    throw new ArgumentOutOfRangeException($"Unexpected binary expression of type: {be.Operator}");
            }
        }

        private IGraphQueryStep BuildQueryPlanForPattern(PatternMatchElementExpression patternExpression)
        {
            var prev = BuildQueryPlanForMatchNode(patternExpression.Path[0]);
            for (int i = 1; i < patternExpression.Path.Length; i+=2)
            {
                var next = i + 1 < patternExpression.Path.Length ?
                    BuildQueryPlanForMatchNode(patternExpression.Path[i + 1]) :
                    null;
                prev = BuildQueryPlanForEdge(prev, next, patternExpression.Path[i]);
            }

            return prev;
        }

        private IGraphQueryStep BuildQueryPlanForEdge(IGraphQueryStep left, IGraphQueryStep right, MatchPath edge)
        {
            var alias = edge.Alias;

            if(edge.Recursive != null)
            {
                return BuildQueryPlanForRecursiveEdge(left, right, edge);
            }

            if (GraphQuery.WithEdgePredicates.TryGetValue(alias, out var withEdge) == false)
            {
                throw new InvalidOperationException($"BuildQueryPlanForEdge was invoked for alias='{alias}' which suppose to be an edge but no corresponding WITH EDGE clause was found.");
            }

            return new EdgeQueryStep(left, right, withEdge, edge, _query.QueryParameters);
        }

        private IGraphQueryStep BuildQueryPlanForRecursiveEdge(IGraphQueryStep left, IGraphQueryStep right, MatchPath edge)
        {
            var recursive = edge.Recursive.Value;
            var pattern = recursive.Pattern;
            var steps = new List<SingleEdgeMatcher>((pattern.Count + 1) / 2);
            for (int i = 0; i < pattern.Count; i += 2)
            {
                if (GraphQuery.WithEdgePredicates.TryGetValue(pattern[i].Alias, out var recursiveEdge) == false)
                {
                    throw new InvalidOperationException($"BuildQueryPlanForEdge was invoked for recursive alias='{pattern[i].Alias}' which suppose to be an edge but no corresponding WITH EDGE clause was found.");
                }

                steps.Add(new SingleEdgeMatcher
                {
                    IncludedEdges = new Dictionary<string, Sparrow.Json.BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase),
                    QueryParameters = _query.QueryParameters,
                    Edge = recursiveEdge,
                    Results = new List<Match>(),
                    Right = i + 1 < pattern.Count ? BuildQueryPlanForMatchNode(pattern[i + 1]) : right,
                    EdgeAlias = pattern[i].Alias
                });
            }

            var recursiveStep = new RecursionQueryStep(left, steps, recursive, recursive.GetOptions(_query.Metadata, _query.QueryParameters));

            if (right == null)
                return recursiveStep;

            return new EdgeFollowingRecursionQueryStep(recursiveStep, right, _query.QueryParameters);
        }

        private IGraphQueryStep BuildQueryPlanForMatchNode(MatchPath vertex)
        {            
            Sparrow.StringSegment alias = vertex.Alias;
            if (GraphQuery.WithDocumentQueries.TryGetValue(alias, out var query) == false)
            {
                throw new InvalidOperationException($"BuildQueryPlanForMatchVertex was invoked for allias='{alias}' which is supposed to be a node but no corresponding WITH clause was found.");
            }
            // TODO: we can tell at this point if it is a collection query or not,
            // TODO: in the future, we want to build a diffrent step for collection queries in the future.        
            var queryMetadata = new QueryMetadata(query, _query.QueryParameters, 0);
            return new QueryQueryStep(_database.QueryRunner, alias, query, queryMetadata, _context, _resultEtag, _token);
        }

        public void OptimizeQueryPlan()
        {
            //TODO: identify pattrens for optimization
            return;
        }

        public List<Match> Execute()
        {
            var list = new List<Match>();
            while (_rootQueryStep.GetNext(out var m))
            {
                list.Add(m);
            }
            return list;
        }
    }
}
