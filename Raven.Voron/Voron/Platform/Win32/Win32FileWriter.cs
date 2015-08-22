// -----------------------------------------------------------------------
//  <copyright file="Win32FileWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Voron.Impl.Paging;
using Voron.Trees;

namespace Voron.Platform.Win32
{
    public unsafe class Win32FileWriter : IFileWriter
    {
        private readonly IVirtualPager _pager;
        private readonly SafeFileHandle _handle;
        private bool _hasWrites;
        private int _numberOfPendingWrites;
        private long _sizeOfPendingWrites;
        private LinkedList<IntPtr> _nativeOverlapped = new LinkedList<IntPtr>();
        private Win32NativeFileMethods.WriteFileCompletionDelegate _writeFileExCompletionRoutine;
        private bool _disposed;
        public Win32FileWriter(string file, IVirtualPager pager)
        {
            _pager = pager;
            _handle = Win32NativeFileMethods.CreateFile(file, Win32NativeFileAccess.GenericWrite,
                Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete, IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways,
                Win32NativeFileAttributes.Overlapped, IntPtr.Zero);

            if (_handle.IsInvalid)
                throw new Win32Exception();
            _writeFileExCompletionRoutine = WriteFileEx_CompletionRoutine;
        }

        public void Dispose()
        {
            _disposed = true; // won't throw from the callback now
            // if called didn't call sync(because of exception, frex), we need to wait for the 
            // pending I/O operations
            while (_numberOfPendingWrites > 0)
            {
                // let the APC queue run, completing all the pending I/Os
                Win32NativeMethods.WaitForSingleObjectEx(_handle.DangerousGetHandle(), Timeout.Infinite, true);
            }

            _handle.Dispose();

            foreach (var ptr in _nativeOverlapped)
            {
                Marshal.FreeHGlobal(ptr);
            }
            _nativeOverlapped.Clear();

            if (_hasWrites)
                ((Win32MemoryMapPager)_pager).RefreshMappedView(null);
        }

        public void Write(Page page)
        {
            _hasWrites = true;
            int pagesToWrite = page.IsOverflow ? _pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;

            _numberOfPendingWrites++;
            _sizeOfPendingWrites += pagesToWrite * AbstractPager.PageSize;
            var offset = page.PageNumber * AbstractPager.PageSize;

            // despite what the docs says, successful call to WriteFileEx won't change the GetLastError on success, I have to assume
            // that depsite that, it is possible that this is triggered somehow, so we manually set the error code to 0 before hand,
            // this way, if there is a call to SetLastError in WriteFileEx, we'll know about it
            Win32NativeMethods.SetLastError(0);
            if (Win32NativeFileMethods.WriteFileEx(_handle, page.Base,
                (uint)(pagesToWrite * AbstractPager.PageSize),
                AllocateOverlappedForOffset(offset), _writeFileExCompletionRoutine) == false)
                throw new Win32Exception();
            var lastWin32Error = Marshal.GetLastWin32Error();
            if (lastWin32Error != 0)
                throw new Win32Exception(lastWin32Error);

            // if we have more than 1K pending writes, or more than 32 MB, we need to let the 
            // system catch up and flush the APC queue
            if (_sizeOfPendingWrites > 1024 * 1024 * 32 ||
                _numberOfPendingWrites > 1024)
            {
                // drain the queue of pending operations by forcing us to wait until something happens
                Win32NativeMethods.WaitForSingleObjectEx(_handle.DangerousGetHandle(), Timeout.Infinite, true);
            }
        }

        private void WriteFileEx_CompletionRoutine(uint dwErrorCode, uint dwNumberOfBytesTransfered, NativeOverlapped* lpOverlapped)
        {
            _nativeOverlapped.AddFirst((IntPtr)lpOverlapped);
            _numberOfPendingWrites--;

            if (dwErrorCode != 0 && _disposed == false)
                throw new Win32Exception((int)dwErrorCode);

            _sizeOfPendingWrites -= dwNumberOfBytesTransfered;
        }

        private NativeOverlapped* AllocateOverlappedForOffset(long offset)
        {
            IntPtr ptr;
            if (_nativeOverlapped.Count == 0)
            {
                ptr = Marshal.AllocHGlobal(sizeof(NativeOverlapped));
            }
            else
            {
                ptr = _nativeOverlapped.First.Value;
                _nativeOverlapped.RemoveFirst();
            }
            var no = (NativeOverlapped*)ptr;
            no->EventHandle = IntPtr.Zero;
            no->InternalHigh = IntPtr.Zero;
            no->InternalLow = IntPtr.Zero;
            no->OffsetHigh = (int)(offset >> 32);
            no->OffsetLow = (int)(offset & 0xffffffff);
            return no;
        }

        public void Sync()
        {
            if (_hasWrites == false)
                return;

            while (_numberOfPendingWrites > 0)
            {
                // let the APC queue run, completing all the pending I/Os
                Win32NativeMethods.WaitForSingleObjectEx(_handle.DangerousGetHandle(), Timeout.Infinite, true);
            }
            if (Win32NativeFileMethods.FlushFileBuffers(_handle) == false)
                throw new Win32Exception();
        }


        public void EnsureContinuous(long pageNumber, int numberOfPagesInLastPage)
        {
            _pager.EnsureContinuous(null, pageNumber, numberOfPagesInLastPage);
        }
    }
}