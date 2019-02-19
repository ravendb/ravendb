using Microsoft.Extensions.Primitives;

namespace Raven.Server.Documents.Queries.AST
{
    public struct FromClause
    {
        public FieldExpression From { get; set; }
        public StringSegment? Alias { get; set; }
        public QueryExpression Filter { get; set; }
        public bool Index { get; set; }
    }
}
