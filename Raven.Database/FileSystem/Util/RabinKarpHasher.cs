//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;

namespace Raven.Database.FileSystem.Util
{
	/// <remarks>
	/// Adapted from http://algs4.cs.princeton.edu/53substring/RabinKarp.java.html
	/// </remarks>
	public class RabinKarpHasher
	{
		private readonly int length;
		private int current;    // pattern hash value
		private int Q = 8355967;          // a large prime, small enough to avoid long overflow
		private int R = 256;           // radix
		private int RM;          // R^(M-1) % Q

		public RabinKarpHasher(int length)
		{
			this.length = length;
			RM = ((int)Math.Pow(R, length - 1)) % Q;
		}

		public int Init(byte[] bytes, int position, int size)
		{
			if (size != length)
				throw new ArgumentException("Buffer size must match hasher length");

			var result = 0;
			for (var i = position; i < position + size; i++)
			{
				var c = bytes[i];
				result = (R * result + c) % Q;
			}

			return result;
		}

		public int Move(byte prev, byte next)
		{
			current = (current + Q - RM * prev % Q) % Q;
			current = (current * R + next) % Q;
			return current;
		}
	}
}