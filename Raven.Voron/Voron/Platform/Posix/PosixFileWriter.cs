// -----------------------------------------------------------------------
//  <copyright file="PosixFileWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.InteropServices;
using Mono.Unix.Native;
using Voron.Impl.Paging;
using Voron.Trees;

namespace Voron.Platform.Posix
{
    public class PosixFileWriter : IFileWriter
    {
        private readonly IVirtualPager _pager;
        private int _fd;
        private bool _hasWrites;

        public PosixFileWriter(string file, IVirtualPager pager)
        {
            _pager = pager;

            _fd = Syscall.open(file, OpenFlags.O_RDWR | OpenFlags.O_CREAT | OpenFlags.O_SYNC,
                             FilePermissions.S_IWUSR | FilePermissions.S_IRUSR);
            if (_fd == -1)
                PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());

        }

        public void Dispose()
        {
            if (_fd != -1)
                Syscall.close(_fd);
        }

        public unsafe void Write(Page page)
        {
            _hasWrites = true;
            var pos = page.PageNumber*AbstractPager.PageSize;
            int pagesToWrite = page.IsOverflow ? _pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;

            long remaining = pagesToWrite*AbstractPager.PageSize;

            var ptr = page.Base;
            while (remaining > 0)
            {
                var written = Syscall.pwrite(_fd, ptr, (ulong)remaining, pos);
                if(written==-1)
                    PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());
                ptr += written;
                remaining -= written;
                pos += written;
            }
        }

        public void Sync()
        {
            if (_hasWrites == false)
                return;
            if(Syscall.fsync(_fd)==-1)
                PosixHelper.ThrowLastError(Marshal.GetLastWin32Error());
        }

        public void EnsureContinuous(long pageNumber, int numberOfPagesInLastPage)
        {
            _pager.EnsureContinuous(null, pageNumber, numberOfPagesInLastPage);
        }
    }
}