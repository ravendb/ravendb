using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Threading;
using Voron.Global;
using Voron.Impl.Paging;
using Voron.Platform;
using Voron.Platform.Win32;
using Voron.Util.Settings;

namespace Voron.Impl.Journal
{
    public unsafe interface IJournalWriter : IDisposable
    {
        void Write(long posBy4Kb, byte* p, int numberOf4Kb);
        int NumberOfAllocated4Kb { get; }
        bool Disposed { get; }
        bool DeleteOnClose { get; set; }
        AbstractPager CreatePager();
        void Read(byte* buffer, long numOfBytes, long offsetInFile);
        void Truncate(long size);

        void AddRef();
        bool Release();

        VoronPathSetting FileName { get; }
    }

    public unsafe class JournalWriter : IJournalWriter
    {
        private readonly SingleUseFlag _disposed = new SingleUseFlag();
        private readonly StorageEnvironmentOptions _options;

        private readonly SafeJournalHandle _writeHandle;
        private SafeJournalHandle _readHandle = new SafeJournalHandle();
        private int _refs;

        public int NumberOfAllocated4Kb { get; }
        public bool Disposed => _disposed.IsRaised();
        public VoronPathSetting FileName { get; }
        public bool DeleteOnClose { get; set; }

        public JournalWriter(StorageEnvironmentOptions options, VoronPathSetting filename, long size, PalFlags.JournalMode mode = PalFlags.JournalMode.Safe)
        {
            _options = options;
            FileName = filename;

            var result = Pal.rvn_open_journal_for_writes(filename.FullPath, mode, size, out _writeHandle, out var actualSize, out var error);
            if (result != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(result, error, $"Attempted to open journal file - Path:{filename.FullPath} Size:{size}");

            NumberOfAllocated4Kb = (int)(actualSize / (4 * Constants.Size.Kilobyte));
        }

        public void Write(long posBy4Kb, byte* p, int numberOf4Kb)
        {
            Debug.Assert(_options.IoMetrics != null);

            using (var metrics = _options.IoMetrics.MeterIoRate(FileName.FullPath, IoMetrics.MeterType.JournalWrite, (long)numberOf4Kb * 4 * Constants.Size.Kilobyte))
            {
                var result = Pal.rvn_write_journal(_writeHandle, p, numberOf4Kb * 4 * Constants.Size.Kilobyte, posBy4Kb * 4 * Constants.Size.Kilobyte, out var error);
                if (result != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(result, error, $"Attempted to write to journal file - Path:{FileName.FullPath} Size:{numberOf4Kb * 4 * Constants.Size.Kilobyte}");

                metrics.SetFileSize(NumberOfAllocated4Kb * (4L * Constants.Size.Kilobyte));
            }
        }

        public AbstractPager CreatePager()
        {
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
                    PalHelper.ThrowLastError(result, errorCode, $"Attempted to read from journal file - Path:{FileName.FullPath} Size:{numOfBytes} Offset:{offsetInFile} ActualSize:{actualSize}");

            }
        }

        public void Truncate(long size)
        {
            var result = Pal.rvn_truncate_journal(_writeHandle, size, out var error);
            if (result != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(result, error, $"Attempted to write to journal file - Path:{FileName.FullPath} Size:{size}");
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
                        $"Attempted to close 'read journal handle' - Path:{FileName.FullPath}");
            });

            TryExecute(() =>
            {
                _writeHandle.Dispose();
                if (_writeHandle.FailCode != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(_writeHandle.FailCode, _writeHandle.ErrorNo,
                        $"Attempted to close 'write journal handle' - Path:{FileName.FullPath}");
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
    
    public class SafeJournalHandle : SafeHandle
    {
        public PalFlags.FailCodes FailCode; 
        public int ErrorNo;
        
        public SafeJournalHandle() : base(IntPtr.Zero, true)
        {
        }

        protected override bool ReleaseHandle()
        {
            FailCode = Pal.rvn_close_journal(handle, out ErrorNo);
            handle = IntPtr.Zero;
            return FailCode == PalFlags.FailCodes.Success;
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }
}
