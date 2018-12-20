using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Sparrow;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Global;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util.Settings;

namespace Voron.Platform.Win32
{
    /// <summary>
    /// This class assumes only a single writer at any given point in time
    /// This require _external_ synchronization
    /// </summary>S
    public unsafe class Win32FileJournalWriter : IJournalWriter
    {
        private readonly StorageEnvironmentOptions _options;
        private readonly VoronPathSetting _filename;
        private SafeFileHandle _handle;
        private SafeFileHandle _readHandle;
        private NativeOverlapped* _nativeOverlapped;
        private SingleUseFlag _disposed = new SingleUseFlag();
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

        public VoronPathSetting FileName => _filename;

        public Win32FileJournalWriter(StorageEnvironmentOptions options, VoronPathSetting filename, long journalSize,
            Win32NativeFileAccess access = Win32NativeFileAccess.GenericWrite,
            Win32NativeFileShare shareMode = Win32NativeFileShare.Read)
        {
            try
            {
                _options = options;
                _filename = filename;
                _handle = Win32NativeFileMethods.CreateFile(filename.FullPath,
                    access, shareMode, IntPtr.Zero,
                    Win32NativeFileCreationDisposition.OpenAlways,
                    options.WinOpenFlags, IntPtr.Zero);

                if (_handle.IsInvalid)
                    throw new IOException("When opening file " + filename, new Win32Exception(Marshal.GetLastWin32Error()));

                var length = new FileInfo(filename.FullPath).Length;
                if (length < journalSize)
                {
                    try
                    {
                        Win32NativeFileMethods.SetFileLength(_handle, journalSize);
                    }
                    catch (Exception)
                    {
                        try
                        {
                            _handle?.Dispose();
                            _handle = null;
                            File.Delete(_filename.FullPath);
                        }
                        catch (Exception)
                        {
                            // there's nothing we can do about it
                        }

                        throw;
                    }

                    length = journalSize;
                }

                NumberOfAllocated4Kb = (int)(length / (4 * Constants.Size.Kilobyte));

                _nativeOverlapped = (NativeOverlapped*)NativeMemory.AllocateMemory(sizeof(NativeOverlapped));

                _nativeOverlapped->InternalLow = IntPtr.Zero;
                _nativeOverlapped->InternalHigh = IntPtr.Zero;
            }
            catch 
            {
                Dispose();
                throw;
            }
        }

        public void Write(long posBy4Kb, byte* p, int numberOf4Kb)
        {
            if (_disposed)
                throw new ObjectDisposedException("Win32JournalWriter");

            const int maxNumberInSingleWrite = (int.MaxValue / (4 * Constants.Size.Kilobyte));
            while (numberOf4Kb > maxNumberInSingleWrite)
            {
                WriteFile(posBy4Kb, p, maxNumberInSingleWrite);

                posBy4Kb += maxNumberInSingleWrite;
                p += maxNumberInSingleWrite * 4 * Constants.Size.Kilobyte;
                numberOf4Kb -= maxNumberInSingleWrite;
            }

            if (numberOf4Kb > 0)
                WriteFile(posBy4Kb, p, numberOf4Kb);
        }

        private int ERROR_WORKING_SET_QUOTA = 1453;// Insufficient quota to complete the requested service.

        private void SetOverlappedPosition(long locationBy4KB)
        {
            var offset = locationBy4KB * 4 * Constants.Size.Kilobyte;
            _nativeOverlapped->OffsetLow = (int)(offset & 0xffffffff);
            _nativeOverlapped->OffsetHigh = (int)(offset >> 32);
            _nativeOverlapped->EventHandle = IntPtr.Zero;
        }

        private void WriteFile(long position, byte* p, int numberOf4Kb)
        {
            Debug.Assert(_options.IoMetrics != null);

            SetOverlappedPosition(position);

            bool writeSuccess;
            var nNumberOfBytesToWrite = numberOf4Kb * (4 * Constants.Size.Kilobyte);
            using (var metrics = _options.IoMetrics.MeterIoRate(_filename.FullPath, IoMetrics.MeterType.JournalWrite, nNumberOfBytesToWrite))
            {
                int written;
                writeSuccess = Win32NativeFileMethods.WriteFile(_handle, p, nNumberOfBytesToWrite,
                    out written,
                    _nativeOverlapped);

                if (writeSuccess == false)
                {
                    HandleWriteError(position, p, numberOf4Kb);
                }

                metrics.SetFileSize(NumberOfAllocated4Kb * (4 * Constants.Size.Kilobyte));
            }
        }

        private void HandleWriteError(long position, byte* p, int numberOf4Kb)
        {
           var errorCode = Marshal.GetLastWin32Error();
            if (errorCode != ERROR_WORKING_SET_QUOTA)
                ThrowOnWriteFileFailure(errorCode);

            // this error can happen under low memory conditions, instead of trying to write the whole thing in a single shot
            // we'll write it in 4KB increments. This is likely to be much slower, but failing here will fail the entire DB
            for (int i = 0; i < numberOf4Kb; i++)
            {
                SetOverlappedPosition(position + i);
                var writeSuccess = Win32NativeFileMethods.WriteFile(_handle, p + (i * (4 * Constants.Size.Kilobyte)), (4 * Constants.Size.Kilobyte),
                    out var written,
                    _nativeOverlapped);

                if (writeSuccess == false)
                    ThrowOnWriteFileFailure(Marshal.GetLastWin32Error());
            }
        }

        private void ThrowOnWriteFileFailure(int errorCode)
        {
            throw new IOException($"Could not write to journal {_filename}, error code: {errorCode}",
                new Win32Exception(errorCode));
        }

        public int NumberOfAllocated4Kb { get; }
        public bool DeleteOnClose { get; set; }

        public AbstractPager CreatePager()
        {
            if (_options.RunningOn32Bits)
                return new Windows32BitsMemoryMapPager(_options, _filename);

            return new WindowsMemoryMapPager(_options, _filename);
        }

        public bool Read(byte* buffer, long numOfBytes, long offsetInFile)
        {
            if (_readHandle == null)
            {
                var handle = Win32NativeFileMethods.CreateFile(_filename.FullPath,
                    Win32NativeFileAccess.GenericRead,
                    Win32NativeFileShare.Write | Win32NativeFileShare.Read | Win32NativeFileShare.Delete,
                    IntPtr.Zero,
                    Win32NativeFileCreationDisposition.OpenExisting,
                    Win32NativeFileAttributes.Normal,
                    IntPtr.Zero);
                if (handle.IsInvalid)
                    throw new IOException("When opening file " + _filename, new Win32Exception(Marshal.GetLastWin32Error()));

                _readHandle = handle;
            }

            var nativeOverlapped = (NativeOverlapped*)NativeMemory.AllocateMemory(sizeof(NativeOverlapped));
            try
            {
                nativeOverlapped->OffsetLow = (int)(offsetInFile & 0xffffffff);
                nativeOverlapped->OffsetHigh = (int)(offsetInFile >> 32);
                nativeOverlapped->EventHandle = IntPtr.Zero;
                while (numOfBytes > 0)
                {
                    if (Win32NativeFileMethods.ReadFile(_readHandle, buffer, (int)Math.Min(numOfBytes, int.MaxValue), out int read, nativeOverlapped) == false)
                    {
                        int lastWin32Error = Marshal.GetLastWin32Error();
                        if (lastWin32Error == Win32NativeFileMethods.ErrorHandleEof)
                            return false;
                        if (lastWin32Error == Win32NativeFileMethods.ErrorInvalidHandle)
                            _readHandle = null;
                        throw new Win32Exception(lastWin32Error, $"Unable to read from {_filename}, error code: {lastWin32Error}");
                    }

                    numOfBytes -= read;
                    buffer += read;
                    offsetInFile += read;
                    nativeOverlapped->OffsetLow = (int)(offsetInFile & 0xffffffff);
                    nativeOverlapped->OffsetHigh = (int)(offsetInFile >> 32);
                }
                return true;
            }
            finally
            {
                NativeMemory.Free((byte*)nativeOverlapped, sizeof(NativeOverlapped));
            }
        }

        public void Dispose()
        {
            if (!_disposed.Raise())
                return;

            GC.SuppressFinalize(this);
            _options.IoMetrics.FileClosed(_filename.FullPath);
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

        public bool Disposed => _disposed.IsRaised();


        ~Win32FileJournalWriter()
        {
            try
            {
                Dispose();
            }
            catch (ObjectDisposedException)
            {
            }

#if DEBUG
            Debug.WriteLine(
                "Disposing a journal file from finalizer! It should be disposed by using JournalFile.Release() instead!. Log file number: "
                + _filename + ". Number of references: " + _refs);
#endif
        }

    }
}

