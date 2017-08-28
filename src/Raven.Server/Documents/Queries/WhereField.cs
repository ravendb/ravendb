using Raven.Server.Documents.Queries.Parser;

namespace Raven.Server.Documents.Queries
{
    public class WhereField
    {
        public readonly bool IsFullTextSearch;

        public WhereField(bool isFullTextSearch)
        {
            IsFullTextSearch = isFullTextSearch;
        }
    }
}
