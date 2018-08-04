using System.Collections.Generic;
using System.Text;
using Raven.Client.Exceptions;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class Query
    {
        public bool IsDistinct;
        public GraphQuery GraphQuery;
        public QueryExpression Where;
        public FromClause From;
        public List<(QueryExpression Expression, StringSegment? Alias)> Select;
        public List<(QueryExpression Expression, StringSegment? Alias)> Load;
        public List<QueryExpression> Include;
        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;
        public List<(QueryExpression Expression, StringSegment? Alias)> GroupBy;

        public Dictionary<StringSegment, (string FunctionText, Esprima.Ast.Program Program)> DeclaredFunctions;

        public string QueryText;
        public (string FunctionText, Esprima.Ast.Program Program) SelectFunctionBody;
        public string UpdateBody;

        public bool TryAddFunction(StringSegment name, (string FunctionText, Esprima.Ast.Program Program) func)
        {
            if (DeclaredFunctions == null)
                DeclaredFunctions = new Dictionary<StringSegment, (string FunctionText, Esprima.Ast.Program Program)>(CaseInsensitiveStringSegmentEqualityComparer.Instance);

            return DeclaredFunctions.TryAdd(name, func);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            new StringQueryVisitor(sb).Visit(this);
            return sb.ToString();
        }

        public void TryAddWithClause((Query Query, StringSegment Allias) withClause)
        {
            if (GraphQuery == null)
            {
                GraphQuery = new GraphQuery();                
            }

            if (GraphQuery.WithDocumentQueries == null)
            {
                GraphQuery.WithDocumentQueries = new Dictionary<StringSegment, Query>();
            }

            if (GraphQuery.WithDocumentQueries.ContainsKey(withClause.Allias)  )
                throw new InvalidQueryException($"Allias {withClause.Allias} is already in use on a diffrent 'With' clause", 
                    QueryText, null);

            GraphQuery.WithDocumentQueries.Add(withClause.Allias, withClause.Query);
        }

        public void TryAddWithEdgePredicates((WithEdgesExpression Expression, StringSegment Allias) WithEdges)
        {
            if (GraphQuery == null)
            {
                GraphQuery = new GraphQuery();               
            }

            if (GraphQuery.WithEdgePredicates == null)
            {
                GraphQuery.WithEdgePredicates = new Dictionary<StringSegment, WithEdgesExpression>();
            }

            if (GraphQuery.WithEdgePredicates.ContainsKey(WithEdges.Allias))
                throw new InvalidQueryException($"Allias {WithEdges.Allias} is already in use on a diffrent 'With' clause",
                    QueryText, null);

            GraphQuery.WithEdgePredicates.Add(WithEdges.Allias, WithEdges.Expression);
        }
    }
}
