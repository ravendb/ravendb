using System;

namespace Raven.Munin
{
	public class ReverseComparableByteArrayWhichIgnoresNull : IComparable<ReverseComparableByteArrayWhichIgnoresNull>, IComparable
	{
		private readonly byte[] inner;

		public ReverseComparableByteArrayWhichIgnoresNull(byte[] inner)
		{
			this.inner = inner;
		}

		public int CompareTo(ReverseComparableByteArrayWhichIgnoresNull other)
		{
			if (inner == null && other.inner == null)
				return 0;
			if (inner == null)
				return 1;
			if (other.inner == null)
				return -1;
			return CompareToImpl(other)*-1;
		}

		private int CompareToImpl(ReverseComparableByteArrayWhichIgnoresNull other)
		{
			if (inner.Length != other.inner.Length)
				return inner.Length - other.inner.Length;
			for (int i = 0; i < inner.Length; i++)
			{
				if (inner[i] != other.inner[i])
					return inner[i] - other.inner[i];
			}
			return 0;
		}

		public int CompareTo(object obj)
		{
			return CompareTo((ReverseComparableByteArrayWhichIgnoresNull)obj);
		}

		public override string ToString()
		{
			if (inner == null)
				return "null";
			return new Guid(inner).ToString();
		}
	}
}