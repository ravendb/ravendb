namespace Raven.Server.Documents.Indexes.MapReduce
{
    public enum MapResultsStorageType : byte
    {
        None,
        Nested = 1,
        Tree = 2
    }
}