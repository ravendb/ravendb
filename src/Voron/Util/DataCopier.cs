// -----------------------------------------------------------------------
//  <copyright file="DataCopyHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using Sparrow;
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

        public void ToStream(AbstractPager src, long startPage, long numberOfPages, Stream output)
        {
            if((_buffer.Length % src.PageSize) != 0)
                throw new ArgumentException("The buffer length must be a multiple of the page size");

            var steps = _buffer.Length/src.PageSize;

            using(var tempTx = new TempPagerTransaction())
            fixed (byte* pBuffer = _buffer)
            {
                for (long i = startPage; i < numberOfPages; i += steps)
                {
                    var pagesToCopy = (int) (i + steps > numberOfPages ? numberOfPages - i : steps);
                    src.EnsureMapped(tempTx, i, pagesToCopy);
                    var ptr = src.AcquirePagePointer(tempTx, i);
                    Memory.Copy(pBuffer, ptr, pagesToCopy*src.PageSize);
                    output.Write(_buffer, 0, pagesToCopy * src.PageSize);

                }
            }
        }


        public void ToStream(StorageEnvironment env, JournalFile journal, long startPage, long pagesToCopy, Stream output)
        {
            var maxNumOfPagesToCopyAtOnce = _buffer.Length/env.Options.PageSize;
            var page = startPage;

            fixed (byte* ptr = _buffer)
            {
                while (pagesToCopy > 0)
                {
                    var pageCount = Math.Min(maxNumOfPagesToCopyAtOnce, pagesToCopy);
                    var bytesCount = (int)(pageCount * env.Options.PageSize);

                    if (journal.JournalWriter.Read(page, ptr, bytesCount) == false)
                         throw new InvalidOperationException("Could not read from journal #" + journal.Number + " " +
                                    +bytesCount + " bytes.");
                    output.Write(_buffer, 0, bytesCount);
                    page += pageCount;
                    pagesToCopy -= pageCount;
                }
            }

            Debug.Assert(pagesToCopy == 0);
        }
    }
}
