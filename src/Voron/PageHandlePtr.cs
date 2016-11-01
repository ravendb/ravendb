namespace Voron
{
    public struct PageHandlePtr
    {
        public readonly long PageNumber;
        public readonly Page Value;
        public readonly bool IsWritable;

        public PageHandlePtr(long pageNumber, Page value, bool isWritable)
        {
            Value = value;
            PageNumber = pageNumber;
            IsWritable = isWritable;
        }
    }
}
