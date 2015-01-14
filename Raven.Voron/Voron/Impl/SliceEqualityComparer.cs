namespace Voron.Impl
{
	using System.Collections.Generic;
    using System.Runtime.CompilerServices;

	public class SliceEqualityComparer : IEqualityComparer<Slice>, IComparer<Slice>
	{
		public static readonly SliceEqualityComparer Instance = new SliceEqualityComparer();
		
		public unsafe bool Equals(Slice x, Slice y)
		{
			return x.Compare(y) == 0;
		}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
		public int GetHashCode(Slice obj)
		{
			return obj.GetHashCode();
		}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
		public int Compare(Slice x, Slice y)
		{
			return x.Compare(y);
		}
	}
}