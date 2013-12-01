// -----------------------------------------------------------------------
//  <copyright file="DataCopyHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using Voron.Impl;
using Voron.Impl.Journal;
using Voron.Impl.Paging;

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

		public void ToStream(JournalFile journal, long startPage, long pagesToCopy, Stream output)
		{
			var maxNumOfPagesToCopyAtOnce = _buffer.Length/AbstractPager.PageSize;
			var page = startPage;

			fixed (byte* ptr = _buffer)
			{
				while (pagesToCopy > 0)
				{
					var pageCount = Math.Min(maxNumOfPagesToCopyAtOnce, pagesToCopy);
					var bytesCount = (int) (pageCount*AbstractPager.PageSize);

					journal.JournalWriter.Read(page, ptr, bytesCount);
					output.Write(_buffer, 0, bytesCount);
					page += pageCount;
					pagesToCopy -= pageCount;
				}
			}

			Debug.Assert(pagesToCopy == 0);
		}
	}
}