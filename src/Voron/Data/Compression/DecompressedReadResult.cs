using System;

namespace Voron.Data.Compression
{
    public class DecompressedReadResult : ReadResult, IDisposable
    {
        private readonly DecompressedLeafPage _page;

        public DecompressedReadResult(ValueReader reader, DecompressedLeafPage page) : base(reader)
        {
            _page = page;
        }

        public void Dispose()
        {
            _page?.Dispose();
        }
    }
}