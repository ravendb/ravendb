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

        public PosixJournalWriter(StorageEnvironmentOptions options, string filename, long journalSize)
        {
            _options = options;
            _filename = filename;


            _fd = Syscall.open(filename, OpenFlags.O_WRONLY | OpenFlags.O_SYNC | OpenFlags.O_CREAT,
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

            NumberOfAllocatedPages = journalSize / _options.PageSize;
        }

        public void Dispose()
        {
            Disposed = true;
            GC.SuppressFinalize(this);
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

        public unsafe void WriteGather(long position, IntPtr[] pages)
        {
            if (pages.Length == 0)
                return; // nothing to do

            var start = 0;
            const int IOV_MAX = 1024;
            while (start < pages.Length)
            {
                var byteLen = 0L;
                var locs = new List<Iovec>
                {
                    new Iovec
                    {
                        iov_base = pages[start],
                        iov_len = (ulong)_options.PageSize
                    }
                };
                start++;
                byteLen += _options.PageSize;
                for (int i = 1; i < pages.Length && locs.Count < IOV_MAX; i++, start++)
                {
                    byteLen += _options.PageSize;
                    var cur = locs[locs.Count - 1];
                    if (((byte*)cur.iov_base.ToPointer() + cur.iov_len) == (byte*)pages[i].ToPointer())
                    {
                        cur.iov_len = cur.iov_len + (ulong)_options.PageSize;
                        locs[locs.Count - 1] = cur;
                    } 
                    else
                    {
                        locs.Add(new Iovec
                        {
                            iov_base = pages[i],
                            iov_len = (ulong)_options.PageSize
                        });
                    }
                }
                
                var result = Syscall.pwritev(_fd, locs.ToArray(), position);
                position += byteLen;
                if (result == -1)
                {
                    var err = Marshal.GetLastWin32Error();
                    PosixHelper.ThrowLastError(err);
                }
            }
        }

        public long NumberOfAllocatedPages { get; private set; }

        public bool Disposed { get; private set; }

        public bool DeleteOnClose { get; set; }

        public IVirtualPager CreatePager()
        {
            return new PosixMemoryMapPager(_options.PageSize,_filename);
        }

        public unsafe bool Read(long pageNumber, byte* buffer, int count)
        {
            if (_fdReads == -1)
            {
                _fdReads = Syscall.open(_filename, OpenFlags.O_RDONLY,FilePermissions.S_IRUSR);
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

        public unsafe void WriteBuffer(long position, byte* srcPosition, int sizeToWrite)
        {
            var offset = Convert.ToUInt64(sizeToWrite);

            var result = Syscall.pwrite(_fd, srcPosition, (ulong)sizeToWrite, (long)offset);

            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }
        }
    }
}
