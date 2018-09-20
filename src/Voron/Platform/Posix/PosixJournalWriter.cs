// -----------------------------------------------------------------------
//  <copyright file="PosixJournalWriter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Sparrow;
using Sparrow.Platform.Posix;
using Voron.Global;
using Voron.Util.Settings;
using static Sparrow.Platform.PlatformDetails;

namespace Voron.Platform.Posix
{
    /// <summary>
    /// This class assume a single writer and requires external syncronization to work properly
    /// </summary>
    public class PosixJournalWriter : IJournalWriter
    {
        private readonly StorageEnvironmentOptions _options;
        private readonly VoronPathSetting _filename;
        private int _fd, _fdReads = -1;
        private readonly int _maxNumberOf4KbPerSingleWrite;
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

        public PosixJournalWriter(StorageEnvironmentOptions options, VoronPathSetting filename, long journalSize)
        {
            _options = options;
            _filename = filename;
            _maxNumberOf4KbPerSingleWrite = int.MaxValue / (4 * Constants.Size.Kilobyte);

           _fd = Syscall.open(filename.FullPath, OpenFlags.O_WRONLY | options.PosixOpenFlags | PerPlatformValues.OpenFlags.O_CREAT,
                FilePermissions.S_IWUSR | FilePermissions.S_IRUSR);

            if (_fd == -1)
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err, "when opening " + filename);
            }

            if (RunningOnMacOsx)
            {
                // mac doesn't support O_DIRECT, we fcntl instead:
                var rc = Syscall.fcntl(_fd, FcntlCommands.F_NOCACHE, (IntPtr)1);
                if (rc != 0)
                {
                    var err = Marshal.GetLastWin32Error();
                    Syscall.ThrowLastError(err, "when fcntl F_NOCACHE for " + filename);
                }
            }

            var length = new FileInfo(filename.FullPath).Length;
            if (length < journalSize)
            {
                length = journalSize;
                try
                {
                    PosixHelper.AllocateFileSpace(options, _fd, journalSize, filename.FullPath);
                }
                catch (Exception)
                {
                    Syscall.close(_fd);
                    throw;
                }
            }
            if (Syscall.CheckSyncDirectoryAllowed(_filename.FullPath) && Syscall.SyncDirectory(filename.FullPath) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err, "when syncing dir for on " + filename);
            }

            NumberOfAllocated4Kb = (int)(length / (4 * Constants.Size.Kilobyte));
        }

        public void Dispose()
        {
            Disposed = true;
            GC.SuppressFinalize(this);
            _options.IoMetrics.FileClosed(_filename.FullPath);
            if (_fdReads != -1)
            {
                Syscall.close(_fdReads);
                _fdReads = -1;
            }
            if (_fd != -1)
            {
                Syscall.close(_fd);
                _fd = -1;
            }
            if (DeleteOnClose)
            {
                _options.TryStoreJournalForReuse(_filename);
            }
        }

        ~PosixJournalWriter()
        {
            Dispose();

#if DEBUG
            Debug.WriteLine(
                "Disposing a journal file from finalizer! It should be disposed by using JournalFile.Release() instead!. Log file number: "
                + _filename + ". Number of references: " + _refs);
#endif
        }


        public unsafe void Write(long posBy4Kb, byte* p, int numberOf4Kb)
        {
            while (numberOf4Kb > _maxNumberOf4KbPerSingleWrite)
            {
                WriteFile(posBy4Kb, p, _maxNumberOf4KbPerSingleWrite);

                posBy4Kb += _maxNumberOf4KbPerSingleWrite;
                p += _maxNumberOf4KbPerSingleWrite * (4 * Constants.Size.Kilobyte);
                numberOf4Kb -= _maxNumberOf4KbPerSingleWrite;
            }

            if (numberOf4Kb > 0)
                WriteFile(posBy4Kb, p, numberOf4Kb);
        }

        private unsafe void WriteFile(long position, byte* p, int numberOf4Kb)
        {
            if (numberOf4Kb == 0)
                return; // nothing to do

            var nNumberOfBytesToWrite = (ulong)numberOf4Kb * (4 * Constants.Size.Kilobyte);
            using (_options.IoMetrics.MeterIoRate(_filename.FullPath, IoMetrics.MeterType.JournalWrite, (long)nNumberOfBytesToWrite))
            {
                Syscall.PwriteOrThrow(_fd, p, nNumberOfBytesToWrite, position * 4 * Constants.Size.Kilobyte, _filename.FullPath, "pwrite journal WriteFile");
            }
        }

        public int NumberOfAllocated4Kb { get; }

        public bool Disposed { get; private set; }

        public bool DeleteOnClose { get; set; }

        public AbstractPager CreatePager()
        {
            if (_options.RunningOn32Bits)
                return new Posix32BitsMemoryMapPager(_options, _filename);

            return new PosixMemoryMapPager(_options, _filename);
        }

        public unsafe bool Read(byte* buffer, long numOfBytes, long offsetInFile)
        {
            if (_fdReads == -1)
            {
                _fdReads = Syscall.open(_filename.FullPath, OpenFlags.O_RDONLY, FilePermissions.S_IRUSR);
                if (_fdReads == -1)
                {
                    var err = Marshal.GetLastWin32Error();
                    Syscall.ThrowLastError(err, "when opening " + _filename);
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
                Syscall.ThrowLastError(err, "when truncating " + _filename);
            }
            result = Syscall.FSync(_fd);
            if (result == -1)
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err, "when fsycning " + _filename);
            }

            if (Syscall.CheckSyncDirectoryAllowed(_filename.FullPath) && Syscall.SyncDirectory(_filename.FullPath) == -1)
            {
                var err = Marshal.GetLastWin32Error();
                Syscall.ThrowLastError(err, "when syncing dir for " + _filename);
            }
        }
    }
}
