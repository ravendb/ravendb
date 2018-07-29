using System.Collections.Generic;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class GraphQuery
    {
        //label -> with query
        //TODO : write a visitor for GraphQuery (don't forget to reuse code from StringQueryVisitor)

        public Dictionary<StringSegment, Query> WithDocumentQueries;
     
        public Dictionary<StringSegment, WithEdgesExpression> WithEdgePredicates;

        public PatternMatchExpression MatchClause;

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
    }
}
