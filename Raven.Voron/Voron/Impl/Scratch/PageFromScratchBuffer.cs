using System.Collections.Generic;
using System.Runtime.CompilerServices;
namespace Voron.Impl.Scratch
{
	public sealed class PageFromScratchBuffer
	{
		public int ScratchFileNumber;
		public long PositionInScratchBuffer;
		public long Size;
		public int NumberOfPages;

        public static readonly EqualityComparer Comparer = new EqualityComparer();

        public sealed class EqualityComparer : IEqualityComparer<PageFromScratchBuffer>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals(PageFromScratchBuffer x, PageFromScratchBuffer y)
            {
                if (x == null || y == null)
                    return false;

                if (x == y)
                    return true;

                return x.PositionInScratchBuffer == y.PositionInScratchBuffer && x.Size == y.Size && x.NumberOfPages == y.NumberOfPages && x.ScratchFileNumber == y.ScratchFileNumber;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetHashCode(PageFromScratchBuffer obj)
            {
                return obj.GetHashCode();
            }
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