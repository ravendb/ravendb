namespace Sparrow.Server.Platform.Posix;

internal struct SmapsReaderNoAllocResults : ISmapsReaderResultAction
{
    public void Add(SmapsReaderResults results)
    {
        // currently we do not use these results with SmapsReaderNoAllocResults so we do not store them
    }
}
