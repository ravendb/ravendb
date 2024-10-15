namespace Sparrow.Server.Platform.Posix;

internal struct SmapsReadResult<T> where T : struct, ISmapsReaderResultAction
{
    public long Rss;
    public long SharedClean;
    public long PrivateClean;
    public long TotalDirty;
    public long Swap;
    public T SmapsResults;
}
