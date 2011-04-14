//-----------------------------------------------------------------------
// <copyright file="ComparableByteArray.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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

    public class ReverseComparable : IComparable<ReverseComparable>, IComparable
    {
        private readonly IComparable inner;

        public ReverseComparable(IComparable inner)
        {
            this.inner = inner;
        }

        public int CompareTo(ReverseComparable other)
        {
            return inner.CompareTo(other.inner)*-1;
        }

        public int CompareTo(object obj)
        {
            return CompareTo((ReverseComparable) obj);
        }
    }
}