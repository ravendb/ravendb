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

namespace Voron.Platform.Win32
{
    /// <summary>
    /// This class assumes only a single writer at any given point in time
    /// This require _external_ synchronization
    /// </summary>
    public unsafe class Win32FileJournalWriter : IJournalWriter
    {


        private readonly string _filename;
        private readonly SafeFileHandle _handle;
        private SafeFileHandle _readHandle;
        private Win32NativeFileMethods.FileSegmentElement* _segments;
        private int _segmentsSize;
        private NativeOverlapped* _nativeOverlapped;

        public static int debugCount = 0;

        public Win32FileJournalWriter(string filename, long journalSize)
        {
            _filename = filename;
            _handle = Win32NativeFileMethods.CreateFile(filename,
                Win32NativeFileAccess.GenericWrite, Win32NativeFileShare.Read, IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways,
                Win32NativeFileAttributes.Write_Through | Win32NativeFileAttributes.NoBuffering | Win32NativeFileAttributes.Overlapped, IntPtr.Zero);

            if (_handle.IsInvalid)
                throw new Win32Exception();

            try
            {
                Win32NativeFileMethods.SetFileLength(_handle, journalSize);
            }
            catch (Exception ex)
            {
                try
                {
                    _handle?.Dispose();
                    _handle = null;
                    File.Delete(_filename);
                }
                catch (Exception)
                {
                    // there's nothing we can do about it
                }
                throw new IOException("When SetFileLength file " + filename + " to " + journalSize, ex);
            }

            NumberOfAllocatedPages = journalSize/AbstractPager.PageSize;

            _nativeOverlapped = (NativeOverlapped*) Marshal.AllocHGlobal(sizeof (NativeOverlapped));

            _nativeOverlapped->InternalLow = IntPtr.Zero;
            _nativeOverlapped->InternalHigh = IntPtr.Zero;

        }

        public void WriteGather(long position, IntPtr[] pages)
        {
            if (Disposed)
                throw new ObjectDisposedException("Win32JournalWriter");

            EnsureSegmentsSize(pages);


            _nativeOverlapped->OffsetLow = (int) (position & 0xffffffff);
            _nativeOverlapped->OffsetHigh = (int) (position >> 32);
            _nativeOverlapped->EventHandle = IntPtr.Zero; // _manualResetEvent.SafeWaitHandle.DangerousGetHandle();

            for (int i = 0; i < pages.Length; i++)
            {
                if (IntPtr.Size == 4)
                    _segments[i].Alignment = (ulong) pages[i];

                else
                    _segments[i].Buffer = pages[i];
            }
            _segments[pages.Length].Alignment = 0; // null terminating

            var operationCompleted = Win32NativeFileMethods.WriteFileGather(_handle, _segments, (uint) pages.Length*4096, IntPtr.Zero, _nativeOverlapped);

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

        private void EnsureSegmentsSize(IntPtr[] pages)
        {
            if (_segmentsSize >= pages.Length + 1)
                return;

            _segmentsSize = (int) Utils.NearestPowerOfTwo(pages.Length + 1);

            if (_segments != null)
                Marshal.FreeHGlobal((IntPtr) _segments);

            _segments = (Win32NativeFileMethods.FileSegmentElement*) (Marshal.AllocHGlobal(_segmentsSize*sizeof (Win32NativeFileMethods.FileSegmentElement)));
        }

        public long NumberOfAllocatedPages { get; private set; }
        public bool DeleteOnClose { get; set; }

        public IVirtualPager CreatePager()
        {
            return new Win32MemoryMapPager(_filename);
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

            long position = pageNumber*AbstractPager.PageSize;
            var overlapped = new Overlapped((int) (position & 0xffffffff), (int) (position >> 32), IntPtr.Zero, null);
            NativeOverlapped* nativeOverlapped = overlapped.Pack(null, null);
            try
            {
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
                Overlapped.Free(nativeOverlapped);
            }
        }

        public void Dispose()
        {
            Disposed = true;
            GC.SuppressFinalize(this);
            if (_readHandle != null)
                _readHandle.Close();

            if (_handle != null)
                _handle.Close();
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

