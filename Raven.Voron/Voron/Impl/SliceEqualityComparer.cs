namespace Voron.Impl
{
	using System.Collections.Generic;

	public class SliceEqualityComparer : IEqualityComparer<Slice>, IComparer<Slice>
	{
		public unsafe bool Equals(Slice x, Slice y)
		{
			return x.Compare(y, NativeMethods.memcmp) == 0;
		}

		public int GetHashCode(Slice obj)
		{
			return obj.GetHashCode();
		}

		public unsafe int Compare(Slice x, Slice y)
		{
			return x.Compare(y, NativeMethods.memcmp);
		}
	}
}