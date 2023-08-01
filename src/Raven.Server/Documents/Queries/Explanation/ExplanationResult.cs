namespace Raven.Server.Documents.Queries.Explanation
{
    public sealed class ExplanationResult
    {
        public string Key;

        public Lucene.Net.Search.Explanation Explanation;
    }
}
