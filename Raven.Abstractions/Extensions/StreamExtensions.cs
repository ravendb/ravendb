//-----------------------------------------------------------------------
// <copyright file="StreamExtension.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Raven.Abstractions.Extensions
{
	/// <summary>
	/// Extensions for working with streams
	/// </summary>
	public static class StreamExtensions
	{
		public static void CopyTo(this Stream stream, Stream other)
		{
			var buffer = new byte[0x1000];
			while(true)
			{
				int read = stream.Read(buffer, 0, buffer.Length);
				if(read == 0)
					return;
				other.Write(buffer, 0, read);
			}
		}

		/// <summary>
		/// Reads the entire request buffer to memory and return it as a byte array.
		/// </summary>
		/// <param name="stream">The stream to read.</param>
		/// <returns>The returned byte array.</returns>
		public static byte[] ReadData(this Stream stream)
		{
			var list = new List<byte[]>();
			const int defaultBufferSize = 1024 * 16;
			var buffer = new byte[defaultBufferSize];
			var currentOffset = 0;
			int read;
			while ((read = stream.Read(buffer, currentOffset, buffer.Length - currentOffset)) != 0)
			{
				currentOffset += read;
				if (currentOffset == buffer.Length)
				{
					list.Add(buffer);
					buffer = new byte[defaultBufferSize];
					currentOffset = 0;
				}
			}
			var totalSize = list.Sum(x => x.Length) + currentOffset;
			var result = new byte[totalSize];
			var resultOffset = 0;
			foreach (var partial in list)
			{
				Buffer.BlockCopy(partial, 0, result, resultOffset, partial.Length);
				resultOffset += partial.Length;
			}
			Buffer.BlockCopy(buffer, 0, result, resultOffset, currentOffset);
			return result;
		}
	}
}