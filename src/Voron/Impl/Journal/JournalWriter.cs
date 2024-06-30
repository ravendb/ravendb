using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Sparrow.Threading;
using Voron.Global;
using Voron.Impl.Paging;
using Voron.Platform.Posix;
using Voron.Platform.Win32;
using Voron.Util.Settings;

namespace Voron.Impl.Journal
{
    public sealed unsafe class JournalWriter : IDisposable
    {
        private const int ERROR_WORKING_SET_QUOTA = 0x5AD;

        private readonly SingleUseFlag _disposed = new SingleUseFlag();
        private readonly StorageEnvironmentOptions _options;

        private readonly SafeJournalHandle _writeHandle;
        private readonly Logger _log;
        private SafeJournalHandle _readHandle = new SafeJournalHandle();
        private int _refs;
        private bool _workingSetQuotaLogged = false;

        public int NumberOfAllocated4Kb { get; }
        public bool Disposed => _disposed.IsRaised();
        public VoronPathSetting FileName { get; }
        public bool DeleteOnClose { get; set; }

        public JournalWriter(StorageEnvironmentOptions options, VoronPathSetting filename, long size, PalFlags.JournalMode mode = PalFlags.JournalMode.Safe)
        {
            _options = options;
            FileName = filename;
            _log = LoggingSource.Instance.GetLogger<JournalWriter>(options.BasePath.FullPath);

            var result = Pal.rvn_open_journal_for_writes(filename.FullPath, mode, size, options.SupportDurabilityFlags, out _writeHandle, out var actualSize, out var error);
            if (result != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(result, error, $"Attempted to open journal file - Path: {filename.FullPath} Size :{size}");

            NumberOfAllocated4Kb = (int)(actualSize / (4 * Constants.Size.Kilobyte));
        }

        public void Write(long posBy4Kb, byte* p, int numberOf4Kb)
        {
            Debug.Assert(_options.IoMetrics != null);

            using (var metrics = _options.IoMetrics.MeterIoRate(FileName.FullPath, IoMetrics.MeterType.JournalWrite, numberOf4Kb * 4L * Constants.Size.Kilobyte))
            {
                var result = Pal.rvn_write_journal(_writeHandle, p, numberOf4Kb * 4L * Constants.Size.Kilobyte, posBy4Kb * 4L * Constants.Size.Kilobyte, out var error);
                if (result != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(result, error, $"Attempted to write to journal file - Path: {FileName.FullPath} Size: {numberOf4Kb * 4L * Constants.Size.Kilobyte}, numberOf4Kb={numberOf4Kb}");

                if (error == ERROR_WORKING_SET_QUOTA && _log.IsOperationsEnabled && _workingSetQuotaLogged == false)
                {
                    _log.Operations(
                        $"We managed to accomplish journal write although we got {nameof(ERROR_WORKING_SET_QUOTA)} under the covers and wrote data in 4KB chunks");

                    _workingSetQuotaLogged = true;
                }

                metrics.SetFileSize(NumberOfAllocated4Kb * (4L * Constants.Size.Kilobyte));
            }
        }

        public (Pager2 Pager, Pager2.State State) CreatePager()
        {
            return Pager2.Create(_options, FileName.FullPath, 0, Pal.OpenFileFlags.None);
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
                PalHelper.ThrowLastError(result, error, $"Attempted to write to journal file - Path: {FileName.FullPath} Size: {size}");
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

            if (DeleteOnClose)
            {
                _options.TryStoreJournalForReuse(FileName);
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
