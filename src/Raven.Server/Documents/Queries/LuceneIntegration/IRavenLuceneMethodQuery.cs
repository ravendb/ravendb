namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    public interface IRavenLuceneMethodQuery
    {
        IRavenLuceneMethodQuery Merge(IRavenLuceneMethodQuery other);
        string Field { get; }
    }
}