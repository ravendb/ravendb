using System.Collections.Generic;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class Query
    {
        public bool IsDistinct;
        public QueryExpression Where;
        public FromClause From;
        public Dictionary<StringSegment, WithClause> WithClauses;
        public MatchClause MatchClauses { get; set; }
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

        public (bool Success, string Error) TryAddWithClause(WithClause with)
        {
            if (WithClauses == null)
            {
                WithClauses = new Dictionary<StringSegment, WithClause>();
            }

            var allias = with.Query.From.Alias;

            if (allias.HasValue == false)
                return (false, "'With' clause must contain an allias.");

            if (WithClauses.ContainsKey(allias.Value)  )
                    return (false, $"Allias {with.Query.From.Alias} is already in use on a diffrent 'With' clause");
            
            WithClauses.Add(allias.Value, with);
            return (true, null);
        }
    }
}
