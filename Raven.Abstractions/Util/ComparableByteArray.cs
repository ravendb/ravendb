// -----------------------------------------------------------------------
//  <copyright file="CBA.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using System.Linq;

namespace Raven.Abstractions.Util
{
	public class ByteArrayComparer : IComparer<Guid>, IComparer<Guid?>
	{
		public static readonly ByteArrayComparer Instance = new ByteArrayComparer();

		public int Compare(Guid x, Guid y)
		{
			return ComparableByteArray.CompareTo(x.ToByteArray(), y.ToByteArray());
		}

		public int Compare(Guid? x, Guid? y)
		{
			if (x == null && y == null)
				return 0;
			if (x == null)
				return -1;
			if (y == null)
				return 1;
			return ComparableByteArray.CompareTo(x.Value.ToByteArray(), y.Value.ToByteArray());
		}
	}

	public class ComparableByteArray : IComparable<ComparableByteArray>, IComparable
	{
		private readonly byte[] inner;

		public ComparableByteArray(Etag etag) : this(etag.ToByteArray())
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
			return CompareTo(inner, otherBuffer);
		}

		public static int CompareTo(byte[] firstBuffer, byte[] otherBuffer)
		{
			if (firstBuffer == null && otherBuffer == null)
				return 0;
			if (firstBuffer == null)
				return 1;
			if (otherBuffer == null)
				return -1;


			if (firstBuffer.Length != otherBuffer.Length)
				return firstBuffer.Length - otherBuffer.Length;
			for (int i = 0; i < firstBuffer.Length; i++)
			{
				if (firstBuffer[i] != otherBuffer[i])
					return firstBuffer[i] - otherBuffer[i];
			}
			return 0;
		}

		public int CompareTo(object obj)
		{
			var comparableByteArray = obj as ComparableByteArray;
			if (comparableByteArray != null)
				return CompareTo(comparableByteArray);
			var etag = obj as Etag;
			if(etag != null)
				return CompareTo((Etag)obj);
			return CompareTo((Guid)obj);
		}

		public int CompareTo(Etag obj)
		{
		    if (obj == null)
		        return -1;
			return CompareTo(obj.ToByteArray());
		}

		public int CompareTo(Guid obj)
		{
			return CompareTo(obj.ToByteArray());
		}

		public Guid ToGuid()
		{
			return new Guid(inner);
		}

		public Etag ToEtag()
		{
			return Etag.Parse(inner);
		}

		public override string ToString()
		{
			return ToGuid().ToString();
		}
	}

}