using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryPlan
    {
        private IQueryStep _rootQueryStep;
        private IndexQueryServerSide _query;
        private DocumentsOperationContext _context;
        private long? _resultEtag;
        private OperationCancelToken _token;
        private DocumentDatabase _database;
        private IntermediateResults _intermediateResults;
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

        private IQueryStep BuildQueryPlanForExpression(QueryExpression expression)
        {
            switch (_query.Metadata.Query.GraphQuery.MatchClause)
            {
                case PatternMatchElementExpression pme:
                    return BuildQueryPlanForPattern(pme);
                case BinaryExpression be:
                    return BuildQueryPlanForBinaryExpression(be);
                default:
                    throw new ArgumentOutOfRangeException($"Unexpected expression of type {expression.Type}");
            }
        }

        private IQueryStep BuildQueryPlanForBinaryExpression(BinaryExpression be)
        {
            var left = BuildQueryPlanForExpression(be.Left);
            var right = BuildQueryPlanForExpression(be.Right);
            switch (be.Operator)
            {
                case OperatorType.And:
                    return new AndQueryStep(left, right);
                case OperatorType.Or:
                    return new OrQueryStep(left, right);
                default:
                    throw new ArgumentOutOfRangeException($"Unexpected binary expression of type: {be.Operator}");
            }
        }

        private IQueryStep BuildQueryPlanForPattern(PatternMatchElementExpression patternExpression)
        {
            IQueryStep prev = BuildQueryPlanForMatchVertex(patternExpression.Path[0]);
            for (var i = 2; i < patternExpression.Path.Length; i+=2)
            {
                var tmpVertex = BuildQueryPlanForMatchVertex(patternExpression.Path[i]);
                prev = BuildQueryPlanForEdge(prev, tmpVertex, patternExpression.Path[i-1]);
            }

            return prev;
        }

        private IQueryStep BuildQueryPlanForEdge(IQueryStep left, IQueryStep right, MatchPath edge)
        {
            var allias = edge.Alias;
            if (GraphQuery.WithEdgePredicates.TryGetValue(allias, out var withEdge) == false)
            {
                throw new InvalidOperationException($"BuildQueryPlanForEdge was invoked for allias='{allias}' which suppose to be an edge but no corresponding WITH EDGE clause was found.");
            }

            return new EdgeQueryStep(left, right, withEdge, edge, _query.QueryParameters);
        }

        private IQueryStep BuildQueryPlanForMatchVertex(MatchPath vertex)
        {            
            Sparrow.StringSegment alias = vertex.Alias;
            if (GraphQuery.WithDocumentQueries.TryGetValue(alias, out var query) == false)
            {
                throw new InvalidOperationException($"BuildQueryPlanForMatchVertex was invoked for allias='{alias}' which suppose to be a vertex but no corresponding WITH clause was found.");
            }
            //TODO: we can tell at this point if it is a collection query or not, we might want to build a diffrent step for collection queries in the future.        
            var queryMetadata = new QueryMetadata(query, _query.QueryParameters, 0);
            return new QueryQueryStep(_database.QueryRunner, alias, query, queryMetadata, _context, _resultEtag, _token);
        }

        public void OptimizeQueryPlan()
        {
            //TODO: identify pattrens for optimization
            return;
        }

        public async ValueTask<IEnumerable<Match>> ExecuteQueryPlan()
        {            
            return await _rootQueryStep.Execute(new Dictionary<IQueryStep, IEnumerable<Match>>());
        }


    }
}
