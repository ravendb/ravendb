using System.Collections.Generic;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class Query
    {
        public bool IsDistinct;
        public QueryExpression Where;
        public (FieldExpression From, StringSegment? Alias, QueryExpression Filter, bool Index) From;
        public List<(QueryExpression Expression, StringSegment? Alias)> Select;
        public List<(QueryExpression Expression, StringSegment? Alias)> Load;
        public List<QueryExpression> Include;
        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;
        public List<FieldExpression> GroupBy;

        public Dictionary<StringSegment, StringSegment> DeclaredFunctions;

        public string QueryText;
        public StringSegment? SelectFunctionBody;
        public StringSegment? UpdateBody;

        public bool TryAddFunction(StringSegment name, StringSegment func)
        {
            if (DeclaredFunctions == null)
                DeclaredFunctions = new Dictionary<StringSegment, StringSegment>(CaseInsensitiveStringSegmentEqualityComparer.Instance);

            return DeclaredFunctions.TryAdd(name, func);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            new StringQueryVisitor(sb).Visit(this);
            return sb.ToString();
        }
    }
}
