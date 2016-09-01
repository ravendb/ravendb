namespace Voron.Impl.Scratch
{
    public sealed class PageFromScratchBuffer
    {
        public readonly int ScratchFileNumber;
        public readonly long PositionInScratchBuffer;
        public readonly long Size;
        public readonly int NumberOfPages;
        public Page PreviousVersion;

        public PageFromScratchBuffer( int scratchFileNumber, long positionInScratchBuffer, long size, int numberOfPages )
        {
            this.ScratchFileNumber = scratchFileNumber;
            this.PositionInScratchBuffer = positionInScratchBuffer;
            this.Size = size;
            this.NumberOfPages = numberOfPages;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;

            var other = (PageFromScratchBuffer)obj;

            return PositionInScratchBuffer == other.PositionInScratchBuffer && Size == other.Size && NumberOfPages == other.NumberOfPages && ScratchFileNumber == other.ScratchFileNumber;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = PositionInScratchBuffer.GetHashCode();
                hashCode = (hashCode * 397) ^ Size.GetHashCode();
                hashCode = (hashCode * 397) ^ NumberOfPages;
                hashCode = (hashCode * 397) ^ ScratchFileNumber;
                return hashCode;
            }
        }

        public override string ToString()
        {
            return string.Format("PositionInScratchBuffer: {0}, ScratchFileNumber: {1},  Size: {2}, NumberOfPages: {3}", PositionInScratchBuffer, ScratchFileNumber, Size, NumberOfPages);
        }
    }
}
