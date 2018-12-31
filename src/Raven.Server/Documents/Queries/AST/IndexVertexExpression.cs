using Microsoft.Extensions.Primitives;

namespace Raven.Server.Documents.Queries.AST
{
    public class IndexVertexExpression: QueryExpression
    {
        public StringSegment IndexName;
        public QueryExpression Filter;
        public StringSegment? Alias;

        public override string ToString()
        {
            var result = $"index '{IndexName}'";

            if (Filter != null)
                result += $" {Filter}";

            if (Alias.HasValue)
                result += $" as {Alias}";

            return result;
        }

        public override string GetText(IndexQueryServerSide parent) => ToString();

        public override bool Equals(QueryExpression other)
        {
            if (other == null || !(other is IndexVertexExpression indexVertexExpression))
                return false;

            if (!(indexVertexExpression.Filter?.Equals(Filter) ?? true))
                return false;

            return indexVertexExpression.IndexName.Equals(IndexName);
        }
    }
}
