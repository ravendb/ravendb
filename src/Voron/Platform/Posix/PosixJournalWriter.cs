// -----------------------------------------------------------------------
//  <copyright file="PosixJournalWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Runtime.InteropServices;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Sparrow;
using Sparrow.Platform.Posix;
using Voron.Global;

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
        private readonly int _maxNumberOf4KbPerSingleWrite;

        public PosixJournalWriter(StorageEnvironmentOptions options, string filename, long journalSize)
        {
            _options = options;
            _filename = filename;
            _maxNumberOf4KbPerSingleWrite = int.MaxValue / (4*Constants.Size.Kilobyte);

            _fd = Syscall.open(filename, OpenFlags.O_WRONLY | options.PosixOpenFlags | OpenFlags.O_CREAT,
                FilePermissions.S_IWUSR | FilePermissions.S_IRUSR);

            if (_fd == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err, "when opening " + filename);
            }

            int result;
            if ((options.SafePosixOpenFlags & PerPlatformValues.OpenFlags.O_DIRECT) == 0)
            {
                // fallocate doesn't supported, we'll use lseek instead
                result = Syscall.AllocateUsingLseek(_fd, journalSize);
            }
            else
            {
                result = Syscall.posix_fallocate(_fd, IntPtr.Zero, (UIntPtr) journalSize);
            }
            if (result != 0)
                PosixHelper.ThrowLastError(result, "when allocating " + filename);

            if (PosixHelper.CheckSyncDirectoryAllowed(_filename) && PosixHelper.SyncDirectory(filename) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err, "when syncing dir for on " + filename);
            }

            NumberOfAllocated4Kb = (int) (journalSize/(4*Constants.Size.Kilobyte));
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


        public unsafe void Write(long position, byte* p, int numberOf4Kb)
        {
            while (numberOf4Kb > _maxNumberOf4KbPerSingleWrite)
            {
                WriteFile(position, p, _maxNumberOf4KbPerSingleWrite);

                position += _maxNumberOf4KbPerSingleWrite;
                p += _maxNumberOf4KbPerSingleWrite * (4*Constants.Size.Kilobyte);
                numberOf4Kb -= _maxNumberOf4KbPerSingleWrite;
            }

            if (numberOf4Kb > 0)
                WriteFile(position, p, numberOf4Kb);
        }

        private unsafe void WriteFile(long position, byte* p, int numberOf4Kb)
        {
            if (numberOf4Kb == 0)
                return; // nothing to do

            var nNumberOfBytesToWrite = (ulong) numberOf4Kb*(4*Constants.Size.Kilobyte);
            long actuallyWritten = 0;
            long result;
            using (_options.IoMetrics.MeterIoRate(_filename, IoMetrics.MeterType.JournalWrite, (long)nNumberOfBytesToWrite))
            {
                do
                {
                    result = Syscall.pwrite(_fd, p, nNumberOfBytesToWrite - (ulong)actuallyWritten, 
                        position * 4 * Constants.Size.Kilobyte);
                    if (result < 1)
                        break;
                    actuallyWritten += result;
                    p += actuallyWritten;
                } while ((ulong)actuallyWritten < nNumberOfBytesToWrite);
            }
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err, "when writing to " + _filename);
            }
            else if (result == 0)
            {
                var err = Marshal.GetLastWin32Error();
                throw new IOException($"pwrite reported zero bytes written, after write of {actuallyWritten} bytes out of {nNumberOfBytesToWrite}. lastErrNo={err} on {_filename}");
            }
            else if ((ulong) actuallyWritten != nNumberOfBytesToWrite)
            {
                var err = Marshal.GetLastWin32Error();
                throw new IOException($"pwrite couln't write {nNumberOfBytesToWrite} to file. only {actuallyWritten} written. lastErrNo={err} on {_filename}");
            }
        }

        public int NumberOfAllocated4Kb { get; }

        public bool Disposed { get; private set; }

        public bool DeleteOnClose { get; set; }

        public AbstractPager CreatePager()
        {
            return new PosixMemoryMapPager(_options, _filename);
        }

        public unsafe bool Read(byte* buffer, long numOfBytes, long offsetInFile)
        {
            if (_fdReads == -1)
            {
                _fdReads = Syscall.open(_filename, OpenFlags.O_RDONLY, FilePermissions.S_IRUSR);
                if (_fdReads == -1)
                {
                    var err = Marshal.GetLastWin32Error();
                    PosixHelper.ThrowLastError(err, "when opening " + _filename);
                }
            }

            while (numOfBytes > 0)
            {
                var result = Syscall.pread(_fdReads, buffer, (ulong)numOfBytes, offsetInFile);
                if (result == 0) //eof
                    return false;
                numOfBytes -= result;
                buffer += result;
                offsetInFile += result;
            }
            return true;
        }

        public void Truncate(long size)
        {
            var result = Syscall.ftruncate(_fd, (IntPtr)size);
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err, "when truncating " + _filename);
            }
            result = Syscall.fsync(_fd);
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err, "when fsycning " + _filename);
            }

            if (PosixHelper.CheckSyncDirectoryAllowed(_filename) && PosixHelper.SyncDirectory(_filename) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err, "when syncing dir for " + _filename);
            }
        }
    }
}
