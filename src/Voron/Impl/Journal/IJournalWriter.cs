using System;
using Sparrow.Threading;
using Voron.Impl.Paging;
using Voron.Platform;
using Voron.Util.Settings;

namespace Voron.Impl.Journal
{
    public unsafe interface IJournalWriter : IDisposable
    {
        void Write(long posBy4Kb, byte* p, int numberOf4Kb);
        int NumberOfAllocated4Kb { get;  }
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

        private readonly IntPtr _handle;

        public int NumberOfAllocated4Kb { get; }
        public bool Disposed => _disposed.IsRaised();
        public VoronPathSetting FileName { get; }
        public bool DeleteOnClose { get; set; }

        public JournalWriter(StorageEnvironmentOptions options, VoronPathSetting filename, long journalSize)
        {
            _options = options;
            FileName = filename;

            Pal.open_journal(filename.FullPath, 0, journalSize, out _handle, out uint error);
        }

        public void Write(long posBy4Kb, byte* p, int numberOf4Kb)
        {
            Pal.write_journal(_handle, (IntPtr)p, (ulong)numberOf4Kb, posBy4Kb, out var errorCode);
        }

        public AbstractPager CreatePager()
        {
            throw new NotImplementedException();
        }

        public bool Read(byte* buffer, long numOfBytes, long offsetInFile)
        {
            throw new NotImplementedException();
        }

        public void Truncate(long size)
        {
            throw new NotImplementedException();
        }

        public void AddRef()
        {
            throw new NotImplementedException();
        }

        public bool Release()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            if (!_disposed.Raise())
                return;
        }
    }
}
