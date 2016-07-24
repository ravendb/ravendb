using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Voron.Exceptions;
using Voron.Impl.Journal;
using Voron.Impl.Paging;

namespace Voron.Platform.Win32
{
    /// <summary>
    /// This class assumes only a single writer at any given point in time
    /// This require _external_ synchronization
    /// </summary>S
    public unsafe class Win32FileJournalWriter : IJournalWriter
    {
        private readonly StorageEnvironmentOptions _options;
        private readonly string _filename;
        private SafeFileHandle _handle;
        private SafeFileHandle _readHandle;
        private NativeOverlapped* _nativeOverlapped;
        private readonly int _maxNumberOfPages;
        private volatile bool _disposed;

        public Win32FileJournalWriter(StorageEnvironmentOptions options, string filename, long journalSize)
        {
            _options = options;
            _filename = filename;
            _handle = Win32NativeFileMethods.CreateFile(filename,
                Win32NativeFileAccess.GenericWrite, Win32NativeFileShare.Read, IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways,
                options.WinOpenFlags, IntPtr.Zero);

            if (_handle.IsInvalid)
                throw new Win32Exception();

            _maxNumberOfPages = int.MaxValue/_options.PageSize;

            Win32NativeFileMethods.SetFileLength(_handle, journalSize);

            NumberOfAllocatedPages = (int)(journalSize / _options.PageSize);

            _nativeOverlapped = (NativeOverlapped*) Marshal.AllocHGlobal(sizeof (NativeOverlapped));

            _nativeOverlapped->InternalLow = IntPtr.Zero;
            _nativeOverlapped->InternalHigh = IntPtr.Zero;
        }

        public void WritePages(long position, byte* p, int numberOfPages)
        {
            if (Disposed)
                throw new ObjectDisposedException("Win32JournalWriter");

            if (numberOfPages > _maxNumberOfPages)
                throw new ArgumentException(
                    "Cannot write " + numberOfPages + " pages in a single write, the size is too large",
                    nameof(numberOfPages));

            _nativeOverlapped->OffsetLow = (int) (position & 0xffffffff);
            _nativeOverlapped->OffsetHigh = (int) (position >> 32);
            _nativeOverlapped->EventHandle = IntPtr.Zero;

            int written;
            var writeSuccess = Win32NativeFileMethods.WriteFile(_handle, p, numberOfPages * _options.PageSize, out written, _nativeOverlapped);

            if (writeSuccess == false)
                throw new VoronUnrecoverableErrorException("Could not write to journal " + _filename,
                    new Win32Exception(Marshal.GetLastWin32Error()));

            // TODO : Measure IO times (RavenDB-4659) - Wrote  {sizeToWrite/1024:#,#} kb in {sp.ElapsedMilliseconds:#,#} ms
           
        }

        public int NumberOfAllocatedPages { get; }
        public bool DeleteOnClose { get; set; }

        public AbstractPager CreatePager()
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

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            GC.SuppressFinalize(this);
            _readHandle?.Dispose();
            _readHandle = null;
            _handle?.Dispose();
            _handle = null;
            if (_nativeOverlapped != null)
            {
                Marshal.FreeHGlobal((IntPtr) _nativeOverlapped);
                _nativeOverlapped = null;
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

        public void Truncate(long size)
        {
            if (Win32NativeFileMethods.FlushFileBuffers(_handle) == false)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to sync for " + _filename);
            Win32NativeFileMethods.SetFileLength(_handle, size);
        }

        public bool Disposed => _disposed;


        ~Win32FileJournalWriter()
        {
            Dispose();
        }

    }
}

