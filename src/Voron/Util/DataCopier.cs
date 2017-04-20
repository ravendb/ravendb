// -----------------------------------------------------------------------
//  <copyright file="DataCopyHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using Sparrow;
using Voron.Global;
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
            // In case of encryption bundle, we don't want to decrypt the data for backup, 
            // so let's work directly with the underlying encrypted data (Inner pager).
         
            if((_buffer.Length % Constants.Storage.PageSize) != 0)
                throw new ArgumentException("The buffer length must be a multiple of the page size");

            var steps = _buffer.Length/ Constants.Storage.PageSize;

            using(var tempTx = new TempPagerTransaction())
            fixed (byte* pBuffer = _buffer)
            {
                for (long i = startPage; i < startPage + numberOfPages; i += steps)
                {
                    var pagesToCopy = (int) (i + steps > numberOfPages ? numberOfPages - i : steps);
                    src.EnsureMapped(tempTx, i, pagesToCopy);
                    var ptr = src.AcquireRawPagePointer(tempTx, i);
                    Memory.Copy(pBuffer, ptr, pagesToCopy* Constants.Storage.PageSize);
                    output.Write(_buffer, 0, pagesToCopy * Constants.Storage.PageSize);

                }
            }
        }


        public void ToStream(StorageEnvironment env, JournalFile journal, long start4Kb, long numberOf4KbsToCopy, Stream output)
        {
            var maxNumOf4KbsToCopyAtOnce = _buffer.Length/(4*Constants.Size.Kilobyte);
            var page = start4Kb;

            fixed (byte* ptr = _buffer)
            {
                while (numberOf4KbsToCopy > 0)
                {
                    var pageCount = Math.Min(maxNumOf4KbsToCopyAtOnce, numberOf4KbsToCopy);

                    if (journal.JournalWriter.Read(ptr, 
                        pageCount * (4 * Constants.Size.Kilobyte), 
                        page * (4 * Constants.Size.Kilobyte)) == false)
                         throw new InvalidOperationException("Could not read from journal #" + journal.Number + " " +
                                    +pageCount + " pages.");
                    var bytesCount = (int)(pageCount * (4 * Constants.Size.Kilobyte));
                    output.Write(_buffer, 0, bytesCount);
                    page += pageCount;
                    numberOf4KbsToCopy -= pageCount;
                }
            }

            Debug.Assert(numberOf4KbsToCopy == 0);
        }
    }
}
