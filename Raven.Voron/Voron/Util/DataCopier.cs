// -----------------------------------------------------------------------
//  <copyright file="DataCopyHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;

namespace Voron.Util
{
	public unsafe class DataCopier
	{
		private readonly byte[] _buffer;

		public DataCopier(int bufferSize)
		{
			_buffer = new byte[bufferSize];
		}

		public void ToStream(byte* ptr, long count, Stream output)
		{
			using (var stream = new UnmanagedMemoryStream(ptr, count))
			{
				while (stream.Position < stream.Length)
				{
					var read = stream.Read(_buffer, 0, _buffer.Length);
					output.Write(_buffer, 0, read);
				}
			}
		}
	}
}