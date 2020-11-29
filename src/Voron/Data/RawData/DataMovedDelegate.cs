namespace Voron.Data.RawData
{
    public unsafe delegate void DataMovedDelegate(long previousId, long newId, byte* data, int size, bool compressed);
}
