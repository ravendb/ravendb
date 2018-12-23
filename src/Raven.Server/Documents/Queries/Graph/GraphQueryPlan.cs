using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryPlan
    {
        public IGraphQueryStep RootQueryStep;
        private IndexQueryServerSide _query;
        private DocumentsOperationContext _context;
        private long? _resultEtag;
        private OperationCancelToken _token;
        private DocumentDatabase _database;
        public bool IsStale { get; set; }
        public long ResultEtag { get; set; }
        public GraphQuery GraphQuery => _query.Metadata.Query.GraphQuery;
        public bool CollectIntermediateResults { get; set; }

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
            RootQueryStep = BuildQueryPlanForExpression(_query.Metadata.Query.GraphQuery.MatchClause);                       
        }

        private IGraphQueryStep BuildQueryPlanForExpression(QueryExpression expression)
        {
            switch (expression)
            {
                case PatternMatchElementExpression pme:
                    return BuildQueryPlanForPattern(pme, 0);
                case BinaryExpression be:
                    return BuildQueryPlanForBinaryExpression(be);
                default:
                    throw new ArgumentOutOfRangeException($"Unexpected expression of type {expression.Type}");
            }
        }

        public void Analyze(List<Match> matches, GraphDebugInfo graphDebugInfo)
        {
            foreach (var match in matches)
            {
                RootQueryStep.Analyze(match, graphDebugInfo);
            }
        }

        public async Task Initialize()
        {
            await RootQueryStep.Initialize();
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
                        return new IntersectionQueryStep<Except>(left, right)
                        {
                            CollectIntermediateResults = CollectIntermediateResults
                        };
                    return new IntersectionQueryStep<Intersection>(left, right, returnEmptyIfRightEmpty:true)
                    {
                        CollectIntermediateResults = CollectIntermediateResults
                    };

               case OperatorType.Or:
                    return new IntersectionQueryStep<Union>(left, right, returnEmptyIfLeftEmpty:false)
                    {
                        CollectIntermediateResults = CollectIntermediateResults
                    };

                default:
                    throw new ArgumentOutOfRangeException($"Unexpected binary expression of type: {be.Operator}");
            }
        }

        private IGraphQueryStep BuildQueryPlanForPattern(PatternMatchElementExpression patternExpression, int start)
        {
            var prev = BuildQueryPlanForMatchNode(patternExpression.Path[start]);
            for (int i = start + 1; i < patternExpression.Path.Length; i+=2)
            {
                if (patternExpression.Path[i].Recursive == null)
                {
                    var next = i + 1 < patternExpression.Path.Length ?
                      BuildQueryPlanForMatchNode(patternExpression.Path[i + 1]) :
                      null;
                    prev = BuildQueryPlanForEdge(prev, next, patternExpression.Path[i]);
                }
                else
                {
                    return BuildQueryPlanForRecursiveEdge(prev, i, patternExpression);
                }
            }

            return prev;
        }

        private IGraphQueryStep BuildQueryPlanForEdge(IGraphQueryStep left, IGraphQueryStep right, MatchPath edge)
        {
            var alias = edge.Alias;

            if (GraphQuery.WithEdgePredicates.TryGetValue(alias, out var withEdge) == false)
            {
                throw new InvalidOperationException($"BuildQueryPlanForEdge was invoked for alias='{alias}' which suppose to be an edge but no corresponding WITH EDGE clause was found.");
            }

            return new EdgeQueryStep(left, right, withEdge, edge, _query.QueryParameters)
            {
                CollectIntermediateResults = CollectIntermediateResults
            };
        }

        private IGraphQueryStep BuildQueryPlanForRecursiveEdge(IGraphQueryStep left, int index, PatternMatchElementExpression patternExpression)
        {
            var recursive = patternExpression.Path[index].Recursive.Value;
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
                    Right = i + 1 < pattern.Count ? BuildQueryPlanForMatchNode(pattern[i + 1]) : null,
                    EdgeAlias = pattern[i].Alias
                });
            }

            var recursiveStep = new RecursionQueryStep(left, steps, recursive, recursive.GetOptions(_query.Metadata, _query.QueryParameters))
            {
                CollectIntermediateResults = CollectIntermediateResults
            };            

            if(index + 1 < patternExpression.Path.Length)
            {
                if (patternExpression.Path[index + 1].IsEdge)
                {
                    var nextPlan = BuildQueryPlanForPattern(patternExpression, index + 2);
                    nextPlan = BuildQueryPlanForEdge(recursiveStep, nextPlan, patternExpression.Path[index + 1]);
                    recursiveStep.SetNext(nextPlan.GetSingleGraphStepExecution());
                }
                else
                {
                    var nextPlan = BuildQueryPlanForPattern(patternExpression, index + 1);
                    recursiveStep.SetNext(nextPlan.GetSingleGraphStepExecution());
                }
            }


            return recursiveStep;
        }

        private IGraphQueryStep BuildQueryPlanForMatchNode(MatchPath node)
        {            
            var alias = node.Alias;
            if (GraphQuery.WithDocumentQueries.TryGetValue(alias, out var query) == false)
            {
                throw new InvalidOperationException($"BuildQueryPlanForMatchVertex was invoked for allias='{alias}' which is supposed to be a node but no corresponding WITH clause was found.");
            }
            // TODO: we can tell at this point if it is a collection query or not,
            // TODO: in the future, we want to build a diffrent step for collection queries in the future.        
            var queryMetadata = new QueryMetadata(query, _query.QueryParameters, 0);
            return new QueryQueryStep(_database.QueryRunner, alias, query, queryMetadata, _query.QueryParameters, _context, _resultEtag, this, _token)
            {
                CollectIntermediateResults = CollectIntermediateResults
            };
        }

        public void OptimizeQueryPlan()
        {
            var cdqsr = new EdgeCollectionDestinationRewriter(_database.DocumentsStorage);
            RootQueryStep = cdqsr.Visit(RootQueryStep);
        }

        public List<Match> Execute()
        {
            var list = new List<Match>();
            while (RootQueryStep.GetNext(out var m))
            {
                list.Add(m);
            }
            return list;
        }

        public async Task<bool> WaitForNonStaleResults()
        {
            if (_context.Transaction == null || _context.Transaction.Disposed)
                _context.OpenReadTransaction();

            var etag = DocumentsStorage.ReadLastEtag(_context.Transaction.InnerTransaction);
            var queryDuration = Stopwatch.StartNew();
            var indexNamesGatherer = new GraphQueryIndexNamesGatherer();
            indexNamesGatherer.Visit(RootQueryStep);
            var indexes = new List<Index>();
            Dictionary<Index, AsyncWaitForIndexing> indexWaiters = new Dictionary<Index, AsyncWaitForIndexing>();
            var queryTimeout = _query.WaitForNonStaleResultsTimeout ?? Index.DefaultWaitForNonStaleResultsTimeout;
            foreach (var indexName in indexNamesGatherer.Indexes)
            {
                var index = _database.IndexStore.GetIndex(indexName);
                indexes.Add(index);
                indexWaiters.Add(index, new AsyncWaitForIndexing(queryDuration, queryTimeout, index));
            }

            var staleIndexes = indexes;
            while (true)
            {
                //If we found a stale index we already disposed of the old transaction
                if (_context.Transaction == null || _context.Transaction.Disposed)
                    _context.OpenReadTransaction();

                //see http://issues.hibernatingrhinos.com/issue/RavenDB-5576
                var frozenAwaiters = new Dictionary<Index, AsyncManualResetEvent.FrozenAwaiter>();
                foreach (var index in staleIndexes)
                {
                    frozenAwaiters.Add(index, index.GetIndexingBatchAwaiter());
                }
                staleIndexes = GetStaleIndexes(staleIndexes, etag);
                //All indexes are not stale we can continue without waiting
                if (staleIndexes.Count == 0)
                {
                    //false means we are not stale
                    return false;
                }

                bool foundStaleIndex = false;
                bool indexTimedout = false;
                //here we will just wait for the first stale index we find
                foreach (var index in staleIndexes)
                {
                    var indexAwaiter = indexWaiters[index];
                    //if any index timedout we are stale
                    indexTimedout |= indexAwaiter.TimeoutExceeded;

                    if (Index.WillResultBeAcceptable(true, _query, indexWaiters[index]) == false)
                    {
                        _context.CloseTransaction();                        
                        await indexAwaiter.WaitForIndexingAsync(frozenAwaiters[index]).ConfigureAwait(false);
                        foundStaleIndex = true;                        
                        break;
                    }
                }

                //We are done waiting for all stale indexes
                if (foundStaleIndex == false)
                {
                    //we might get here if all indexes have timedout 
                    return indexTimedout;
                }
            }
        }

        private List<Index> GetStaleIndexes(List<Index> indexes, long etag)
        {
            var staleList = new List<Index>();
            foreach (var index in indexes)
            {
                if (index.IsStale(_context, etag))
                {
                    staleList.Add(index);
                }
            }

            return staleList;
        }

    }
}
