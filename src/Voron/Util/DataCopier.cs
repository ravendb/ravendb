// -----------------------------------------------------------------------
//  <copyright file="DataCopyHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
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

        public void ToStream(AbstractPager src, long startPage, long numberOfPages, 
            Stream output, Action<string> infoNotify, CancellationToken cancellationToken)
        {
            // In case of encryption, we don't want to decrypt the data for backup, 
            // so let's work directly with the underlying encrypted data (Inner pager).
         
            if((_buffer.Length % Constants.Storage.PageSize) != 0)
                throw new ArgumentException("The buffer length must be a multiple of the page size");

            var steps = _buffer.Length/ Constants.Storage.PageSize;
            long totalCopied = 0;
            var toBeCopied = new Size(numberOfPages * Constants.Storage.PageSize, SizeUnit.Bytes).ToString();
            var totalSw = Stopwatch.StartNew();
            var sw = Stopwatch.StartNew();

            using (var tempTx = new TempPagerTransaction())
            fixed (byte* pBuffer = _buffer)
            {
                for (var i = startPage; i < startPage + numberOfPages; i += steps)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var pagesToCopy = (int) (i + steps > numberOfPages ? numberOfPages - i : steps);
                    src.EnsureMapped(tempTx, i, pagesToCopy);
                    var ptr = src.AcquireRawPagePointer(tempTx, i);
                    var copiedInBytes = pagesToCopy * Constants.Storage.PageSize;
                    Memory.Copy(pBuffer, ptr, copiedInBytes);
                    output.Write(_buffer, 0, copiedInBytes);

                    totalCopied += copiedInBytes;

                    if (sw.ElapsedMilliseconds > 500)
                    {
                        infoNotify($"Copied: {new Size(totalCopied, SizeUnit.Bytes)} / {toBeCopied}");
                        sw.Restart();
                    }  
                }
            }

            var totalSecElapsed = Math.Max((double)totalSw.ElapsedMilliseconds / 1000, 0.0001);
            infoNotify?.Invoke($"Finshed copying {new Size(totalCopied, SizeUnit.Bytes)}, " +
                                $"{new Size((long)(totalCopied / totalSecElapsed), SizeUnit.Bytes)}/sec");
        }


        public void ToStream(StorageEnvironment env, JournalFile journal, long start4Kb, 
            long numberOf4KbsToCopy, Stream output, Action<string> infoNotify = null, CancellationToken cancellationToken = default)
        {
            const int pageSize = 4 * Constants.Size.Kilobyte;
            var maxNumOf4KbsToCopyAtOnce = _buffer.Length / pageSize;
            var page = start4Kb;
            var toBeCopied = new Size(numberOf4KbsToCopy * pageSize, SizeUnit.Bytes).ToString();
            var totalSw = Stopwatch.StartNew();
            var sw = Stopwatch.StartNew();
            long totalCopied = 0;

            long pageCount = 0;
            try
            {
                fixed (byte* ptr = _buffer)
                {
                    while (numberOf4KbsToCopy > 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        pageCount = Math.Min(maxNumOf4KbsToCopyAtOnce, numberOf4KbsToCopy);

                        journal.JournalWriter.Read(ptr, pageCount * pageSize, page * pageSize);
                        
                        var bytesCount = (int)(pageCount * (4 * Constants.Size.Kilobyte));
                        output.Write(_buffer, 0, bytesCount);
                        page += pageCount;
                        numberOf4KbsToCopy -= pageCount;

                        totalCopied += bytesCount;
                        if (sw.ElapsedMilliseconds > 500)
                        {
                            infoNotify?.Invoke($"Copied: {new Size(totalCopied, SizeUnit.Bytes)} / {toBeCopied}");
                            sw.Restart();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(
                    "Could not read from journal #" + journal.Number + " " + pageCount + " pages.", e);
            }

            var totalSecElapsed = Math.Max((double)totalSw.ElapsedMilliseconds / 1000, 0.0001);
            infoNotify?.Invoke($"Finshed copying {new Size(totalCopied, SizeUnit.Bytes)}, " +
                                $"{new Size((long)(totalCopied / totalSecElapsed), SizeUnit.Bytes)}/sec");

            Debug.Assert(numberOf4KbsToCopy == 0);
        }
    }
}
