using System.Collections.Generic;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class GraphQuery
    {
        public Dictionary<StringSegment, Query> WithDocumentQueries;
     
        public Dictionary<StringSegment, WithEdgesExpression> WithEdgePredicates;

        public QueryExpression MatchClause;

        public QueryExpression Where;

        public List<QueryExpression> Include;

        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;

        public Dictionary<StringSegment, (string FunctionText, Esprima.Ast.Program Program)> DeclaredFunctions;

        public string QueryText;

        public (string FunctionText, Esprima.Ast.Program Program) SelectFunctionBody;

        public bool TryAddFunction(StringSegment name, (string FunctionText, Esprima.Ast.Program Program) func)
        {
            if (DeclaredFunctions == null)
                DeclaredFunctions = new Dictionary<StringSegment, (string FunctionText, Esprima.Ast.Program Program)>(CaseInsensitiveStringSegmentEqualityComparer.Instance);

            return DeclaredFunctions.TryAdd(name, func);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            new StringQueryVisitor(sb).VisitGraph(this);
            return sb.ToString();
        }
    }
}
