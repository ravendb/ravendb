using Raven.Server.Documents.Queries.Parser;

namespace Raven.Server.Documents.Queries
{
    public class WhereField
    {
        public readonly ValueTokenType Type;

        public readonly bool IsFullTextSearch;

        public WhereField(ValueTokenType type, bool isFullTextSearch)
        {
            Type = type;
            IsFullTextSearch = isFullTextSearch;
        }
    }
}
