using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;
using Sparrow.Binary;

namespace Voron.Platform.Win32
{
    /// <summary>
    /// This class assumes only a single writer at any given point in time
    /// This require _external_ synchronization
    /// </summary>
    public unsafe class Win32FileJournalWriter : IJournalWriter
    {
        private readonly StorageEnvironmentOptions _options;
        private readonly int _pageSizeMultiplier;
        private readonly string _filename;
        private readonly SafeFileHandle _handle;
        private SafeFileHandle _readHandle;
        private Win32NativeFileMethods.FileSegmentElement* _segments;
        private int _segmentsSize;
        private NativeOverlapped* _nativeOverlapped;

        private const int PhysicalPageSize = 4 * Constants.Size.Kilobyte;

        public Win32FileJournalWriter(StorageEnvironmentOptions options, string filename, long journalSize)
        {
            _options = options;
            _filename = filename;
            _handle = Win32NativeFileMethods.CreateFile(filename,
                Win32NativeFileAccess.GenericWrite, Win32NativeFileShare.Read, IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways,
                Win32NativeFileAttributes.Write_Through | Win32NativeFileAttributes.NoBuffering | Win32NativeFileAttributes.Overlapped, IntPtr.Zero);

            _pageSizeMultiplier = options.PageSize / PhysicalPageSize;

            if (_handle.IsInvalid)
                throw new Win32Exception();

            Win32NativeFileMethods.SetFileLength(_handle, journalSize);

            NumberOfAllocatedPages = journalSize / _options.PageSize;

            _nativeOverlapped = (NativeOverlapped*) Marshal.AllocHGlobal(sizeof (NativeOverlapped));

            _nativeOverlapped->InternalLow = IntPtr.Zero;
            _nativeOverlapped->InternalHigh = IntPtr.Zero;
        }

        public void WriteGather(long position, IntPtr[] pages)
        {
            if (Disposed)
                throw new ObjectDisposedException("Win32JournalWriter");

            var physicalPages = EnsureSegmentsSize(pages);

            _nativeOverlapped->OffsetLow = (int) (position & 0xffffffff);
            _nativeOverlapped->OffsetHigh = (int) (position >> 32);
            _nativeOverlapped->EventHandle = IntPtr.Zero;

            PreparePagesToWrite(pages);            
          
            // WriteFileGather will only be able to write x pages of size GetSystemInfo().dwPageSize. Usually that is 4096 (4kb). If you are
            // having trouble with this method, ensure that this value havent changed for your environment. 
            var operationCompleted = Win32NativeFileMethods.WriteFileGather(_handle, _segments, (uint)(physicalPages * PhysicalPageSize), IntPtr.Zero, _nativeOverlapped);

            uint lpNumberOfBytesWritten;

            if (operationCompleted)
            {
                if (Win32NativeFileMethods.GetOverlappedResult(_handle, _nativeOverlapped, out lpNumberOfBytesWritten, true) == false)
                    throw new VoronUnrecoverableErrorException("Could not write to journal " + _filename, new Win32Exception(Marshal.GetLastWin32Error()));
                return;
            }

            switch (Marshal.GetLastWin32Error())
            {
                case Win32NativeFileMethods.ErrorSuccess:
                case Win32NativeFileMethods.ErrorIOPending:
                    if (Win32NativeFileMethods.GetOverlappedResult(_handle, _nativeOverlapped, out lpNumberOfBytesWritten, true) == false)
                        throw new VoronUnrecoverableErrorException("Could not write to journal " + _filename, new Win32Exception(Marshal.GetLastWin32Error()));
                    break;
                default:
                    throw new VoronUnrecoverableErrorException("Could not write to journal " + _filename, new Win32Exception(Marshal.GetLastWin32Error()));
            }
        }

        private void PreparePagesToWrite(IntPtr[] pages)
        {
            if (_pageSizeMultiplier == 1)
            {
                for (int i = 0; i < pages.Length; i++)
                {
                    if (IntPtr.Size == 4)
                        _segments[i].Alignment = (ulong)pages[i];

                    else
                        _segments[i].Buffer = pages[i];
                }

                _segments[pages.Length].Alignment = 0; // null terminating
                return;
            }

            var pageLength = pages.Length*_pageSizeMultiplier;
            for (int step = 0; step < _pageSizeMultiplier; step++)
            {
                int offset = step * PhysicalPageSize;
                for (int i = 0, ptr = step; i < pages.Length; i++, ptr += _pageSizeMultiplier)
                {
                    if (IntPtr.Size == 4)
                        _segments[ptr].Alignment = (ulong)(pages[i] + offset);

                    else
                        _segments[ptr].Buffer = pages[i] + offset;
                }
            }

            _segments[pageLength].Alignment = 0; // null terminating
        }

        private int EnsureSegmentsSize(IntPtr[] pages)
        {
            int physicalPages = (pages.Length * _pageSizeMultiplier);
            if (_segmentsSize >= physicalPages + 1)
                return physicalPages;

            _segmentsSize = Bits.NextPowerOf2(physicalPages + 1);

            if (_segments != null)
                Marshal.FreeHGlobal((IntPtr) _segments);

            _segments = (Win32NativeFileMethods.FileSegmentElement*) (Marshal.AllocHGlobal(_segmentsSize*sizeof (Win32NativeFileMethods.FileSegmentElement)));
            return physicalPages;
        }

        public long NumberOfAllocatedPages { get; private set; }
        public bool DeleteOnClose { get; set; }

        public IVirtualPager CreatePager()
        {
            return new Win32MemoryMapPager(_options.PageSize,_filename);
        }

        public bool Read(long pageNumber, byte* buffer, int count)
        {
            if (_readHandle == null)
            {
                _readHandle = Win32NativeFileMethods.CreateFile(_filename,
                    Win32NativeFileAccess.GenericRead,
                    Win32NativeFileShare.Write | Win32NativeFileShare.Read | Win32NativeFileShare.Delete,
                    IntPtr.Zero,
                    Win32NativeFileCreationDisposition.OpenExisting,
                    Win32NativeFileAttributes.Normal,
                    IntPtr.Zero);
            }

            long position = pageNumber * _options.PageSize;
            NativeOverlapped* nativeOverlapped = (NativeOverlapped*)Marshal.AllocHGlobal(sizeof(NativeOverlapped));
            try
            {
                nativeOverlapped->OffsetLow = (int)(position & 0xffffffff);
                nativeOverlapped->OffsetHigh = (int) (position >> 32);
                nativeOverlapped->EventHandle = IntPtr.Zero;
                while (count > 0)
                {
                    int read;
                    if (Win32NativeFileMethods.ReadFile(_readHandle, buffer, count, out read, nativeOverlapped) == false)
                    {
                        int lastWin32Error = Marshal.GetLastWin32Error();
                        if (lastWin32Error == Win32NativeFileMethods.ErrorHandleEof)
                            return false;
                        throw new Win32Exception(lastWin32Error);
                    }
                    count -= read;
                    buffer += read;
                    position += read;
                    nativeOverlapped->OffsetLow = (int) (position & 0xffffffff);
                    nativeOverlapped->OffsetHigh = (int) (position >> 32);
                }
                return true;
            }
            finally
            {
                Marshal.FreeHGlobal((IntPtr) nativeOverlapped);
            }
        }

        public void WriteBuffer(long position, byte* srcPointer, int sizeToWrite)
        {
            if (Disposed)
                throw new ObjectDisposedException("Win32JournalWriter");

            int written;

            _nativeOverlapped->OffsetLow = (int)(position & 0xffffffff);
            _nativeOverlapped->OffsetHigh = (int)(position >> 32);
            _nativeOverlapped->EventHandle = IntPtr.Zero;

            var operationCompleted = Win32NativeFileMethods.WriteFile(_handle, srcPointer, sizeToWrite, out written, _nativeOverlapped);

            uint lpNumberOfBytesWritten;

            if (operationCompleted)
            {
                if (Win32NativeFileMethods.GetOverlappedResult(_handle, _nativeOverlapped, out lpNumberOfBytesWritten, true) == false)
                    throw new VoronUnrecoverableErrorException("Could not write lazy buffer to journal " + _filename, new Win32Exception(Marshal.GetLastWin32Error()));
                return;
            }

            switch (Marshal.GetLastWin32Error())
            {
                case Win32NativeFileMethods.ErrorSuccess:
                case Win32NativeFileMethods.ErrorIOPending:
                    if (Win32NativeFileMethods.GetOverlappedResult(_handle, _nativeOverlapped, out lpNumberOfBytesWritten, true) == false)
                        throw new VoronUnrecoverableErrorException("Could not write lazy buffer to journal " + _filename, new Win32Exception(Marshal.GetLastWin32Error()));
                    break;
                default:
                    throw new VoronUnrecoverableErrorException("Could not write lazy buffer to journal " + _filename, new Win32Exception(Marshal.GetLastWin32Error()));
            }
        }

        public void Dispose()
        {
            Disposed = true;
            GC.SuppressFinalize(this);
            if (_readHandle != null)
                _readHandle.Dispose();
            _handle.Dispose();
            if (_nativeOverlapped != null)
            {
                Marshal.FreeHGlobal((IntPtr) _nativeOverlapped);
                _nativeOverlapped = null;
            }
            if (_segments != null)
            {
                Marshal.FreeHGlobal((IntPtr) _segments);
                _segments = null;
            }

            if (DeleteOnClose)
            {
                try
                {
                    File.Delete(_filename);
                }
                catch (Exception)
                {
                    // if we can't delete, nothing that we can do here.
                }
            }
        }

        public bool Disposed { get; private set; }


        ~Win32FileJournalWriter()
        {
            Dispose();
        }

    }
}

