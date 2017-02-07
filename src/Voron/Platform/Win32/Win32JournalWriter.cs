using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Sparrow;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Global;
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
        private volatile bool _disposed;
        private int _refs;

        public void AddRef()
        {
            Interlocked.Increment(ref _refs);
        }

        public bool Release()
        {
            if (Interlocked.Decrement(ref _refs) != 0)
                return false;

            Dispose();
            return true;
        }

        public Win32FileJournalWriter(StorageEnvironmentOptions options, string filename, long journalSize, 
            Win32NativeFileAccess access = Win32NativeFileAccess.GenericWrite, 
            Win32NativeFileShare shareMode = Win32NativeFileShare.Read)
        {
            _options = options;
            _filename = filename;
            _handle = Win32NativeFileMethods.CreateFile(filename,
                access, shareMode, IntPtr.Zero,
                Win32NativeFileCreationDisposition.OpenAlways,
                options.WinOpenFlags, IntPtr.Zero);

            if (_handle.IsInvalid)
                throw new IOException("When opening file " + filename, new Win32Exception(Marshal.GetLastWin32Error()));

            var length = new FileInfo(filename).Length;
            if (length < journalSize)
            {
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
                length = journalSize;
            }

            NumberOfAllocated4Kb = (int) (length / (4*Constants.Size.Kilobyte));

            _nativeOverlapped = (NativeOverlapped*) NativeMemory.AllocateMemory(sizeof (NativeOverlapped));

            _nativeOverlapped->InternalLow = IntPtr.Zero;
            _nativeOverlapped->InternalHigh = IntPtr.Zero;
        }

        public void Write(long posBy4Kb, byte* p, int numberOf4Kb)
        {
            if (Disposed)
                throw new ObjectDisposedException("Win32JournalWriter");

            const int maxNumberInSingleWrite = (int.MaxValue/(4*Constants.Size.Kilobyte));
            while (numberOf4Kb > maxNumberInSingleWrite)
            {
                WriteFile(posBy4Kb, p, maxNumberInSingleWrite);

                posBy4Kb += maxNumberInSingleWrite;
                p += maxNumberInSingleWrite * 4 *Constants.Size.Kilobyte;
                numberOf4Kb -= maxNumberInSingleWrite;
            }

            if (numberOf4Kb > 0)
                WriteFile(posBy4Kb, p, numberOf4Kb);
        }

        private void WriteFile(long position, byte* p, int numberOf4Kb)
        {
            position *= 4*Constants.Size.Kilobyte;
            _nativeOverlapped->OffsetLow = (int)(position & 0xffffffff);
            _nativeOverlapped->OffsetHigh = (int)(position >> 32);
            _nativeOverlapped->EventHandle = IntPtr.Zero;

            Debug.Assert(_options.IoMetrics != null);

            bool writeSuccess;
            var nNumberOfBytesToWrite = numberOf4Kb*(4*Constants.Size.Kilobyte);
            using (_options.IoMetrics.MeterIoRate(_filename, IoMetrics.MeterType.JournalWrite, nNumberOfBytesToWrite))
            {
                int written;
                writeSuccess = Win32NativeFileMethods.WriteFile(_handle, p, nNumberOfBytesToWrite,
                    out written,
                    _nativeOverlapped);
            }

            if (writeSuccess == false)
                throw new IOException("Could not write to journal " + _filename,
                    new Win32Exception(Marshal.GetLastWin32Error()));
        }

        public int NumberOfAllocated4Kb { get; }
        public bool DeleteOnClose { get; set; }

        public AbstractPager CreatePager()
        {
            return new Win32MemoryMapPager(_options, _filename);
            //return new SparseMemoryMappedPager(_options,_filename);
        }

        public bool Read(byte* buffer, long numOfBytes, long offsetInFile)
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
                if(_readHandle.IsInvalid)
                    throw new IOException("When opening file " + _filename, new Win32Exception(Marshal.GetLastWin32Error()));
            }

            NativeOverlapped* nativeOverlapped = (NativeOverlapped*)NativeMemory.AllocateMemory(sizeof(NativeOverlapped));
            try
            {
                nativeOverlapped->OffsetLow = (int)(offsetInFile & 0xffffffff);
                nativeOverlapped->OffsetHigh = (int) (offsetInFile >> 32);
                nativeOverlapped->EventHandle = IntPtr.Zero;
                while (numOfBytes > 0)
                {
                    int read;
                    if (Win32NativeFileMethods.ReadFile(_readHandle, buffer, (int)Math.Min(numOfBytes, int.MaxValue), out read, nativeOverlapped) == false)
                    {
                        int lastWin32Error = Marshal.GetLastWin32Error();
                        if (lastWin32Error == Win32NativeFileMethods.ErrorHandleEof)
                            return false;
                        throw new Win32Exception(lastWin32Error, "Unable to read from " + _filename);
                    }
                    numOfBytes -= read;
                    buffer += read;
                    offsetInFile += read;
                    nativeOverlapped->OffsetLow = (int) (offsetInFile & 0xffffffff);
                    nativeOverlapped->OffsetHigh = (int) (offsetInFile >> 32);
                }
                return true;
            }
            finally
            {
                NativeMemory.Free((byte*) nativeOverlapped, sizeof(NativeOverlapped));
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            GC.SuppressFinalize(this);
            _options.IoMetrics.FileClosed(_filename);
            _readHandle?.Dispose();
            _readHandle = null;
            _handle?.Dispose();
            _handle = null;
            if (_nativeOverlapped != null)
            {
                NativeMemory.Free((byte*)_nativeOverlapped, sizeof(NativeOverlapped));
                _nativeOverlapped = null;
            }
           
            if (DeleteOnClose)
            {
                _options.TryStoreJournalForReuse(_filename);
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

#if DEBUG
            Debug.WriteLine(
                "Disposing a journal file from finalizer! It should be disposed by using JournalFile.Release() instead!. Log file number: "
                + _filename + ". Number of references: " + _refs);
#endif
        }

    }
}

