using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Raven.Http.Extensions
{
	/// <summary>
	/// Extensions for working with streams
	/// </summary>
	public static class StreamExtension
	{

		/// <summary>
		/// 	Reads the entire request buffer to memory and
		/// 	return it as a byte array.
		/// </summary>
		public static byte[] ReadData(this Stream steram)
		{
			var list = new List<byte[]>();
			const int defaultBufferSize = 1024 * 16;
			var buffer = new byte[defaultBufferSize];
			var offset = 0;
			int read;
			while ((read = steram.Read(buffer, offset, buffer.Length - offset)) != 0)
			{
				offset += read;
				if (offset == buffer.Length)
				{
					list.Add(buffer);
					buffer = new byte[defaultBufferSize];
					offset = 0;
				}
			}
			var totalSize = list.Sum(x => x.Length) + offset;
			var result = new byte[totalSize];
			var resultOffset = 0;
			foreach (var partial in list)
			{
				Buffer.BlockCopy(partial, 0, result, resultOffset, partial.Length);
				resultOffset += partial.Length;
			}
			Buffer.BlockCopy(buffer, 0, result, resultOffset, offset);
			return result;
		}

	}
}
