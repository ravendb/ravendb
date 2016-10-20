// -----------------------------------------------------------------------
//  <copyright file="PosixJournalWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Runtime.InteropServices;
using Voron.Impl;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using System.Collections.Generic;
using Sparrow;

namespace Voron.Platform.Posix
{
    /// <summary>
    /// This class assume a single writer and requires external syncronization to work properly
    /// </summary>
    public class PosixJournalWriter : IJournalWriter
    {
        private readonly StorageEnvironmentOptions _options;
        private readonly string _filename;
        private int _fd, _fdReads = -1;
        private readonly int _maxNumberOfPagesPerSingleWrite;

        public PosixJournalWriter(StorageEnvironmentOptions options, string filename, long journalSize)
        {
            _options = options;
            _filename = filename;
            _maxNumberOfPagesPerSingleWrite = int.MaxValue / _options.PageSize;

            _fd = Syscall.open(filename, OpenFlags.O_WRONLY | options.PosixOpenFlags | OpenFlags.O_CREAT,
                FilePermissions.S_IWUSR | FilePermissions.S_IRUSR);

            if (_fd == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }

            var result = Syscall.posix_fallocate(_fd, 0, (ulong)journalSize);
            if (result != 0)
                PosixHelper.ThrowLastError(result);

            if (PosixHelper.CheckSyncDirectoryAllowed(_filename) && PosixHelper.SyncDirectory(filename) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }

            NumberOfAllocatedPages = (int)(journalSize / _options.PageSize);
        }

        public void Dispose()
        {
            Disposed = true;
            GC.SuppressFinalize(this);
            _options.IoMetrics.FileClosed(_filename);
            if (_fdReads != -1)
            {
                Syscall.close(_fdReads);
                _fdReads = -1;
            }
            Syscall.close(_fd);
            _fd = -1;
            if (DeleteOnClose)
            {
                try
                {
                    File.Delete(_filename);
                }
                catch (Exception)
                {
                    // nothing to do here
                }
            }
        }

        ~PosixJournalWriter()
        {
            Dispose();
        }


        public unsafe void WritePages(long position, byte* p, int numberOfPages)
        {
            while (numberOfPages > _maxNumberOfPagesPerSingleWrite)
            {
                WriteFile(position, p, _maxNumberOfPagesPerSingleWrite);

                var nextChunkPosition = _maxNumberOfPagesPerSingleWrite * _options.PageSize;
                position += nextChunkPosition;
                p += nextChunkPosition;
                numberOfPages -= _maxNumberOfPagesPerSingleWrite;
            }

            if (numberOfPages > 0)
                WriteFile(position, p, numberOfPages);
        }

        private unsafe void WriteFile(long position, byte* p, int numberOfPages)
        {
            if (numberOfPages == 0)
                return; // nothing to do

            var nNumberOfBytesToWrite = (ulong)numberOfPages*(ulong)_options.PageSize;
            long actuallyWritten = 0;
            long result;
            using (_options.IoMetrics.MeterIoRate(_filename, IoMetrics.MeterType.JournalWrite, (long)nNumberOfBytesToWrite))
            {
                do
                {
                    result = Syscall.pwrite(_fd, p, nNumberOfBytesToWrite - (ulong)actuallyWritten, position);
                    if (result < 1)
                        break;
                    actuallyWritten += result;
                    p += actuallyWritten;
                } while ((ulong)actuallyWritten < nNumberOfBytesToWrite);
            }
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }
            else if (result == 0)
            {
                var err = Marshal.GetLastWin32Error();
                throw new IOException($"pwrite reported zero bytes written, after write of {actuallyWritten} bytes out of {nNumberOfBytesToWrite}. lastErrNo={err}");
            }
            else if ((ulong) actuallyWritten != nNumberOfBytesToWrite)
            {
                var err = Marshal.GetLastWin32Error();
                throw new IOException($"pwrite couln't write {nNumberOfBytesToWrite} to file. only {actuallyWritten} written. lastErrNo={err}");
            }
        }

        public int NumberOfAllocatedPages { get; }

        public bool Disposed { get; private set; }

        public bool DeleteOnClose { get; set; }

        public AbstractPager CreatePager()
        {
            return new PosixMemoryMapPager(_options, _filename);
        }

        public unsafe bool Read(long pageNumber, byte* buffer, int count)
        {
            if (_fdReads == -1)
            {
                _fdReads = Syscall.open(_filename, OpenFlags.O_RDONLY, FilePermissions.S_IRUSR);
                if (_fdReads == -1)
                {
                    var err = Marshal.GetLastWin32Error();
                    PosixHelper.ThrowLastError(err);
                }
            }
            long position = pageNumber * _options.PageSize;

            while (count > 0)
            {
                var result = Syscall.pread(_fdReads, buffer, (ulong)count, position);
                if (result == 0) //eof
                    return false;
                count -= (int)result;
                buffer += result;
                position += result;
            }
            return true;
        }

        public void Truncate(long size)
        {
            var result = Syscall.ftruncate(_fd, size);
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }
            result = Syscall.fsync(_fd);
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }

            if (PosixHelper.CheckSyncDirectoryAllowed(_filename) && PosixHelper.SyncDirectory(_filename) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }
        }
    }
}
