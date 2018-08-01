using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryRunner
    {
        protected DocumentDatabase Database;        

        public GraphQueryRunner([NotNull] DocumentDatabase database)
        {
            Database = database ?? throw new ArgumentNullException(nameof(database));
        }

        public async Task<DocumentQueryResult> RunAsync(GraphQuery q)
        {
            var withQueryRunner = new DynamicQueryRunner(Database);
            //first do the with clause queries
            var withClauseResults = new Dictionary<StringSegment, DocumentQueryResult>();
            foreach (var withClause in q.WithDocumentQueries)
            {
                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                {
                    var withClauseQueryResult =
                        await withQueryRunner.ExecuteQuery(new IndexQueryServerSide(withClause.Value.ToString()), ctx, null, OperationCancelToken.None);
                    withClauseResults.Add(withClause.Key, withClauseQueryResult);
                }
            }

            var patternMatchPairs = new List<(PatternMatchVertexExpression from, (StringSegment? Alias, StringSegment? Type) edge, PatternMatchVertexExpression to)>();

            new MatchPatternExpressionVisitor(
                (PatternMatchVertexExpression from, 
                 (StringSegment? Alias, StringSegment? Type) edge, 
                 PatternMatchVertexExpression to) =>
            {
                patternMatchPairs.Add((from, edge, to));
            }).Visit(q);

            return new DocumentQueryResult
            {
            };
        }
    }
}
