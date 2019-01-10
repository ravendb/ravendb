using System;
using System.Diagnostics;
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
        bool Read(byte* buffer, long numOfBytes, long offsetInFile);
        void Truncate(long size);

        void AddRef();
        bool Release();

        VoronPathSetting FileName { get; }
    }

    public unsafe class JournalWriter : IJournalWriter
    {
        private readonly SingleUseFlag _disposed = new SingleUseFlag();
        private readonly StorageEnvironmentOptions _options;

        private readonly IntPtr _writeHandle;
        //TODO Maybe should init handle to invalid value in pal function?
        //TODO Maybe should move read functionality from this class
        //TODO OR maybe should to reduce read code to one function that will open and close the read handle
        private IntPtr _readHandle = new IntPtr(-1);
        private int _refs;

        public int NumberOfAllocated4Kb { get; }
        public bool Disposed => _disposed.IsRaised();
        public VoronPathSetting FileName { get; }
        public bool DeleteOnClose { get; set; }

        public JournalWriter(StorageEnvironmentOptions options, VoronPathSetting filename, long size, PalFlags.JOURNAL_MODE mode = PalFlags.JOURNAL_MODE.SAFE)
        {
            _options = options;
            FileName = filename;

            var result = Pal.rvn_open_journal(filename.FullPath, (int)mode, size, out _writeHandle, out var actualSize, out var error);
            if (result != (int)PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(error, $"Attempted to open journal file - Path:{filename.FullPath} Size:{size}");

            NumberOfAllocated4Kb = (int)(actualSize / (4 * Constants.Size.Kilobyte));
        }

        public void Write(long posBy4Kb, byte* p, int numberOf4Kb)
        {
            Debug.Assert(_options.IoMetrics != null);

            using (var metrics = _options.IoMetrics.MeterIoRate(FileName.FullPath, IoMetrics.MeterType.JournalWrite, (long)numberOf4Kb * 4 * Constants.Size.Kilobyte))
            {
                var result = Pal.rvn_write_journal(_writeHandle, (IntPtr)p, (ulong)numberOf4Kb * 4 * Constants.Size.Kilobyte, posBy4Kb * 4 * Constants.Size.Kilobyte, out var error);
                if (result != (int)PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(error, $"Attempted to write to journal file - Path:{FileName.FullPath} Size:{numberOf4Kb * 4 * Constants.Size.Kilobyte}");

                metrics.SetFileSize(NumberOfAllocated4Kb * (4L * Constants.Size.Kilobyte));
            }
        }

        public AbstractPager CreatePager()
        {
            if (_options.RunningOn32Bits)
                return new Windows32BitsMemoryMapPager(_options, FileName);

            return new WindowsMemoryMapPager(_options, FileName);
        }

        public bool Read(byte* buffer, long numOfBytes, long offsetInFile)
        {
            var result = Pal.rvn_read_journal(
                FileName.FullPath,
                ref _readHandle,
                buffer,
                (ulong)numOfBytes,
                offsetInFile,
                out var actualSize,
                out var error
                );

            if (result != (int)PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(error, $"Attempted to read from journal file - Path:{FileName.FullPath} Size:{numOfBytes} Offset:{offsetInFile} ActualSize:{actualSize}");

            return actualSize >= (ulong)numOfBytes;
        }

        public void Truncate(long size)
        {
            var result = Pal.rvn_truncate_journal(FileName.FullPath, _writeHandle, (ulong)size, out var error);
            if (result != (int)PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(error, $"Attempted to write to journal file - Path:{FileName.FullPath} Size:{size}");
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

            int error;
            if (_readHandle != IntPtr.Zero)
            {
                var rResult = Pal.rvn_close_journal(_readHandle, out error);
                if (rResult != (int)PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rResult,
                        $"Attempted to close read journal handle - Path:{FileName.FullPath}");

                _readHandle = IntPtr.Zero;
            }

            var result = Pal.rvn_close_journal(_writeHandle, out error);
            if (result != (int)PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(error,
                    $"Attempted to close read journal handle - Path:{FileName.FullPath}");

            if (DeleteOnClose)
            {
                _options.TryStoreJournalForReuse(FileName);
            }
        }

        [Conditional("DEBUG")]
        private static void ValidateNoError(int error, string msg)
        {
            if (error != (int)PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(error, msg);
        }

        ~JournalWriter()
        {
            if (_disposed.IsRaised())
                return;

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
                + FileName + ". Number of references: " + _refs);
#endif
        }
    }
}
