// -----------------------------------------------------------------------
//  <copyright file="Win32FileWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel;
using Microsoft.Win32.SafeHandles;
using Voron.Impl.Paging;
using Voron.Trees;

namespace Voron.Platform.Win32
{
    public class Win32FileWriter : IFileWriter
    {
        private readonly IVirtualPager _pager;
        private readonly SafeFileHandle _handle;
        private bool _hasWrites;
        public Win32FileWriter(string file, IVirtualPager pager)
        {
            _pager = pager;
            _handle = Win32NativeFileMethods.CreateFile(file, Win32NativeFileAccess.GenericWrite,
                Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete, IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways, Win32NativeFileAttributes.Normal, IntPtr.Zero);
        }

        public void Dispose()
        {
            _handle.Dispose();
            if(_hasWrites)
                ((Win32MemoryMapPager)_pager).RefreshMappedView(null);
        }

        public unsafe void Write(Page page)
        {
            _hasWrites = true;
            if (Win32NativeFileMethods.SetFilePointerEx(_handle, page.PageNumber * AbstractPager.PageSize, IntPtr.Zero,
                   Win32NativeFileMoveMethod.Begin) == false)
                throw new Win32Exception();

            int pagesToWrite = page.IsOverflow ? _pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;
            int remaining = pagesToWrite * AbstractPager.PageSize;
            
            var ptr = page.Base;
            while (remaining > 0)
            {
                int written;
                if (Win32NativeFileMethods.WriteFile(_handle, ptr, remaining, out written, null) == false)
                    throw new Win32Exception();
                ptr += written;
                remaining -= written;
            }
        }

        public void Sync()
        {
            if (_hasWrites == false)
                return;

            if(Win32NativeFileMethods.FlushFileBuffers(_handle) == false)
                throw new Win32Exception();
        }


        public void EnsureContinuous(long pageNumber, int numberOfPagesInLastPage)
        {
            _pager.EnsureContinuous(null, pageNumber, numberOfPagesInLastPage);
        }
    }
}