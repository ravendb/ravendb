namespace Raven.Server.Documents.Indexes.Workers
{
    public enum IndexingWorkType
    {
        None,
        Cleanup,
        References,
        Map,
        Reduce
    }
}
