// -----------------------------------------------------------------------
//  <copyright file="CBA.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Util
{
	public class ComparableByteArray : IComparable<ComparableByteArray>, IComparable
	{
		private readonly byte[] inner;

		public ComparableByteArray(Guid etag) : this(etag.ToByteArray())
		{
			
		}

		public ComparableByteArray(byte[] inner)
		{
			this.inner = inner;
		}

		public int CompareTo(ComparableByteArray other)
		{
			var otherBuffer = other.inner;
			return CompareTo(otherBuffer);
		}

		public int CompareTo(byte[] otherBuffer)
		{
			if (inner == null && otherBuffer == null)
				return 0;
			if (inner == null)
				return 1;
			if (otherBuffer == null)
				return -1;


			if (inner.Length != otherBuffer.Length)
				return inner.Length - otherBuffer.Length;
			for (int i = 0; i < inner.Length; i++)
			{
				if (inner[i] != otherBuffer[i])
					return inner[i] - otherBuffer[i];
			}
			return 0;
		}

		public int CompareTo(object obj)
		{
			return CompareTo((ComparableByteArray)obj);
		}

		public int CompareTo(Guid obj)
		{
			return CompareTo(obj.ToByteArray());
		}

		public Guid ToGuid()
		{
			return new Guid(inner);
		}

		public override string ToString()
		{
			return ToGuid().ToString();
		}
	}

}