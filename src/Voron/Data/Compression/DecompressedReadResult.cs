using System;

namespace Voron.Data.Compression
{
    public sealed class DecompressedReadResult : IDisposable
    {
        private readonly DecompressedLeafPage _page;

        public DecompressedReadResult(ValueReader reader, DecompressedLeafPage page)
        {
            _page = page;
            Reader = reader;
        }
        
        public ValueReader Reader { get; private set; }

        public void Dispose()
        {
            _page?.Dispose();
        }
    }
}
