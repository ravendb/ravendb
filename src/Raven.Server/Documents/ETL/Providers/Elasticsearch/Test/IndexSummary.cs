namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Test
{
    public sealed class IndexSummary
    {
        public string IndexName { get; set; }

        public string[] Commands { get; set; }
    }
}
