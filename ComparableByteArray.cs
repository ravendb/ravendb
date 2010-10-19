using System;

namespace Raven.Munin
{
    public class ComparableByteArray : IComparable<ComparableByteArray>, IComparable
    {
        private readonly byte[] inner;

        public ComparableByteArray(byte[] inner)
        {
            this.inner = inner;
        }

        public int CompareTo(ComparableByteArray other)
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
            return CompareTo((ComparableByteArray) obj);
        }
    }
}