using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Logging;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Sparrow.Threading;
using Voron.Global;
using Voron.Impl.Paging;
using Voron.Logging;
using Voron.Platform.Posix;
using Voron.Platform.Win32;
using Voron.Util.Settings;

namespace Voron.Impl.Journal
{
    public sealed unsafe class JournalWriter : IDisposable
    {
        private const int ERROR_WORKING_SET_QUOTA = 0x5AD;

        private readonly SingleUseFlag _disposed = new();
        private readonly StorageEnvironmentOptions _options;
        private readonly long _journalNumber;

        private readonly SafeJournalHandle _writeHandle;
        private readonly RavenLogger _log;
        private SafeJournalHandle _readHandle = new();
        private int _refs;
        private bool _workingSetQuotaLogged = false;

        public int NumberOfAllocated4Kb { get; private set; }
        public bool Disposed => _disposed.IsRaised();
        public VoronPathSetting FileName { get; }
        public bool ShouldDelete { get; set; }

        /// <summary>
        /// This is used when we want to create a branch writer from an existing JournalFile in the root environment 
        /// </summary>
        public JournalWriter(StorageEnvironmentOptions options, string filename, long journalNumber, JournalFile sourceFile)
        {
            _options = options;
            FileName = new VoronPathSetting(filename);
            _journalNumber = journalNumber;
            _writeHandle = new SafeJournalHandle();
            NumberOfAllocated4Kb = (int)(sourceFile.JournalSize.GetValue(SizeUnit.Bytes) / Constants.Storage.JournalPageSize);
        }
        
        public JournalWriter(StorageEnvironmentOptions options, VoronPathSetting filename, long journalNumber, long size, PalFlags.JournalMode mode = PalFlags.JournalMode.Safe)
        {
            _options = options;
            _journalNumber = journalNumber;
            FileName = filename;
            _log = RavenLogManager.Instance.GetLoggerForVoron<JournalWriter>(options, options.BasePath.FullPath);

            var result = Pal.rvn_open_journal_for_writes(filename.FullPath, mode, size, options.SupportDurabilityFlags, out _writeHandle, out var actualSize, out var error);
            if (result != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(result, error, $"Attempted to open journal file - Path: {filename.FullPath} Size :{size}");

            NumberOfAllocated4Kb = (int)(actualSize / Constants.Storage.JournalPageSize);
        }

        public void Write(long posBy4Kb, Span<Pal.journal_entry> entries, long totalNumberOf4Kbs)
        {
            Debug.Assert(_options.IoMetrics != null);

            fixed (Pal.journal_entry* pEntries = entries)
            {
                using var metrics = _options.IoMetrics.MeterIoRate(FileName.FullPath, IoMetrics.MeterType.JournalWrite,
                    totalNumberOf4Kbs * Constants.Storage.JournalPageSize);
                var result = Pal.rvn_write_journal(_writeHandle, pEntries, entries.Length, posBy4Kb * Constants.Storage.JournalPageSize, out var error);
                if (result != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(result, error,
                        $"Attempted to write to journal file - Path: {FileName.FullPath} Size: {totalNumberOf4Kbs * Constants.Storage.JournalPageSize}, numberOf4Kb={totalNumberOf4Kbs}");

                if (error == ERROR_WORKING_SET_QUOTA && _log.IsDebugEnabled && _workingSetQuotaLogged == false)
                {
                    _log.Debug(
                        $"We managed to accomplish journal write although we got {nameof(ERROR_WORKING_SET_QUOTA)} under the covers and wrote data in 4KB chunks");

                    _workingSetQuotaLogged = true;
                }

                metrics.SetFileSize(NumberOfAllocated4Kb * Constants.Storage.JournalPageSize);
            }
        }

        public (Pager Pager, Pager.State State) CreatePager()
        {
            var flags = Pal.OpenFileFlags.None;
            if(_options.ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                flags |= Pal.OpenFileFlags.DoNotMap;
            return Pager.Create(_options, FileName.FullPath, 0, flags, pageSize: Constants.Storage.JournalPageSize);
        }

        public void Read(byte* buffer, long numOfBytes, long offsetInFile)
        {
            int errorCode;
            long actualSize = 0;
            PalFlags.FailCodes result;
            if (_readHandle.IsInvalid)
            {
                result = Pal.rvn_open_journal_for_reads(FileName.FullPath, out _readHandle, out errorCode);
                EnsureValidResult();
            }

            result = Pal.rvn_read_journal(
                _readHandle,
                buffer,
                numOfBytes,
                offsetInFile,
                out actualSize,
                out errorCode
                );

            EnsureValidResult();

            void EnsureValidResult()
            {
                if (result != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(result, errorCode, $"Attempted to read from journal file - Path: {FileName.FullPath} Size: {numOfBytes} Offset: {offsetInFile} ActualSize: {actualSize}");

            }
        }

        public void Truncate(long size)
        {
            var result = Pal.rvn_truncate_journal(_writeHandle, size, out var error);
            if (result != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(result, error, $"Attempted to truncate journal file - Path: {FileName.FullPath} Size: {size}");
            NumberOfAllocated4Kb = checked((int)(size / Constants.Storage.JournalPageSize));
        }

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

        public void Dispose()
        {
            if (!_disposed.Raise())
                return;

            GC.SuppressFinalize(this);
            _options.IoMetrics.FileClosed(FileName.FullPath);

            List<Exception> exceptions = null;

            TryExecute(() =>
            {
                _readHandle.Dispose();
                if (_readHandle.FailCode != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(_readHandle.FailCode, _readHandle.ErrorNo,
                        $"Attempted to close 'read journal handle' - Path: {FileName.FullPath}");
            });

            TryExecute(() =>
            {
                _writeHandle.Dispose();
                if (_writeHandle.FailCode != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(_writeHandle.FailCode, _writeHandle.ErrorNo,
                        $"Attempted to close 'write journal handle' - Path: {FileName.FullPath}");
            });

            if (exceptions != null)
                throw new AggregateException("Failed to dispose journal writer", exceptions);

            if (ShouldDelete)
            {
                _options.TryDeleteJournal(_journalNumber);
            }

            void TryExecute(Action a)
            {
                try
                {
                    a();
                }
                catch (Exception e)
                {
                    if (exceptions == null)
                        exceptions = new List<Exception>();
                    exceptions.Add(e);
                }
            }
        }

        ~JournalWriter()
        {
            try
            {
                Dispose();
            }
            catch (Exception)
            {
                // ignored
            }

#if DEBUG
            Debug.WriteLine(
                "Disposing a journal file from finalizer! It should be disposed by using JournalFile.Release() instead!. Log file number: "
                + FileName + ". Number of references: " + _refs);
#endif
        }
    }
}
