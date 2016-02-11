namespace Raven.Server.Queries.LuceneIntegration
{
    public interface IRavenLuceneMethodQuery
    {
        IRavenLuceneMethodQuery Merge(IRavenLuceneMethodQuery other);
        string Field { get; }
    }
}