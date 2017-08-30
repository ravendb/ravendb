namespace Raven.Server.Documents.Queries
{
    public class WhereField
    {
        public readonly bool IsFullTextSearch;

        public readonly bool IsExactSearch;

        public WhereField(bool isFullTextSearch, bool isExactSearch)
        {
            IsFullTextSearch = isFullTextSearch;
            IsExactSearch = isExactSearch;
        }
    }
}
