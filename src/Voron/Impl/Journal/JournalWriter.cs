using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Sparrow.Threading;
using Voron.Global;
using Voron.Impl.Paging;
using Voron.Platform;
using Voron.Platform.Posix;
using Voron.Platform.Win32;
using Voron.Util.Settings;

namespace Voron.Impl.Journal
{
    public unsafe class JournalWriter : IJournalWriter
    {
        private const int ERROR_WORKING_SET_QUOTA = 0x5AD;

        private readonly SingleUseFlag _disposed = new SingleUseFlag();
        private readonly StorageEnvironmentOptions _options;

        private readonly SafeJournalHandle _writeHandle;
        private readonly Logger _log;
        private SafeJournalHandle _readHandle = new SafeJournalHandle();
        private int _refs;
        private bool _workingSetQuotaLogged = false;
        private readonly StorageEnvironmentSynchronization _writeSynchronization;
        private readonly long _maxNumberOf4KbBlocks;

        public int NumberOfAllocated4Kb { get; }
        public bool Disposed => _disposed.IsRaised();
        public VoronPathSetting FileName { get; }
        public bool DeleteOnClose { get; set; }

        public JournalWriter(StorageEnvironmentOptions options, VoronPathSetting filename, long size, StorageEnvironmentSynchronization writeSynchronization = null, PalFlags.JournalMode mode = PalFlags.JournalMode.Safe)
        {
            _options = options;            
            FileName = filename;
            _log = LoggingSource.Instance.GetLogger<JournalWriter>(options.BasePath.FullPath);

            if (writeSynchronization == null)
                writeSynchronization = new StorageEnvironmentSynchronization();

            // The write synchronization mechanism will require a max size and to be aligned to 4kb.  
            _writeSynchronization = writeSynchronization;
            _maxNumberOf4KbBlocks = writeSynchronization.MaxWriteSize.GetValue(SizeUnit.Kilobytes) / 4L;

            var result = Pal.rvn_open_journal_for_writes(filename.FullPath, mode, size, options.SupportDurabilityFlags, out _writeHandle, out var actualSize, out var error);
            if (result != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(result, error, $"Attempted to open journal file - Path: {filename.FullPath} Size :{size}");

            NumberOfAllocated4Kb = (int)(actualSize / (4L * Constants.Size.Kilobyte));
        }

        public TimeSpan Write(long posBy4Kb, byte* buffer, long numberOf4KbBlocks)
        {
            Debug.Assert(_options.IoMetrics != null);

            Stopwatch sp;
            using (var metrics = _options.IoMetrics.MeterIoRate(FileName.FullPath, IoMetrics.MeterType.JournalWrite, numberOf4KbBlocks * 4L * Constants.Size.Kilobyte))
            {
                sp = Stopwatch.StartNew();

                byte* currentBufferPtr = buffer;
                while (numberOf4KbBlocks > 0)
                {
                    long blocksToWrite = Math.Min(numberOf4KbBlocks, _maxNumberOf4KbBlocks);

                    IoMeterBuffer.DurationMeasurement writeMetrics = _options.IoMetrics.MeterIoRate(FileName.FullPath, IoMetrics.MeterType.JournalWait, blocksToWrite * 4L * Constants.Size.Kilobyte);
                    using (_writeSynchronization.Enter())
                    {
                        writeMetrics.SetFileSize(blocksToWrite * (4L * Constants.Size.Kilobyte));
                        writeMetrics.Dispose();

                        var result = Pal.rvn_write_journal(_writeHandle, currentBufferPtr, blocksToWrite * 4L * Constants.Size.Kilobyte, posBy4Kb * 4L * Constants.Size.Kilobyte, out var error);
                        if (result != PalFlags.FailCodes.Success)
                            PalHelper.ThrowLastError(result, error, $"Attempted to write to journal file - Path: {FileName.FullPath} Size: {numberOf4KbBlocks * 4L * Constants.Size.Kilobyte}, numberOf4Kb={numberOf4KbBlocks}");

                        if (error == ERROR_WORKING_SET_QUOTA && _log.IsOperationsEnabled && _workingSetQuotaLogged == false)
                        {
                            _log.Operations(
                                $"We managed to accomplish journal write although we got {nameof(ERROR_WORKING_SET_QUOTA)} under the covers and wrote data in 4KB chunks");

                            _workingSetQuotaLogged = true;
                        }
                    }

                    posBy4Kb += blocksToWrite;
                    currentBufferPtr += blocksToWrite * (4L * Constants.Size.Kilobyte);

                    numberOf4KbBlocks -= blocksToWrite;
                }

                sp.Stop();

                metrics.SetFileSize(NumberOfAllocated4Kb * (4L * Constants.Size.Kilobyte));
            }

            return sp.Elapsed;
        }

        public AbstractPager CreatePager()
        {
            if (PlatformDetails.RunningOnPosix)
            {
                if (_options.RunningOn32Bits)
                    return new Posix32BitsMemoryMapPager(_options, FileName);

                return new RvnMemoryMapPager(_options, FileName);
            }

            if (_options.RunningOn32Bits)
                return new Windows32BitsMemoryMapPager(_options, FileName);

            return new WindowsMemoryMapPager(_options, FileName);
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
