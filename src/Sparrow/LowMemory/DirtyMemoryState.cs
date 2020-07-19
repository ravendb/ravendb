namespace Sparrow.LowMemory
{
    public class DirtyMemoryState 
    {
        public bool IsHighDirty;

        public long TotalDirtyInBytes;

        public long TotalDirtyInMb => new Size(TotalDirtyInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
    }
}
