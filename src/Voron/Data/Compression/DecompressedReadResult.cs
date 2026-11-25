using System;

namespace Voron.Data.Compression
{
    /// <summary>
    /// A value read from a tree with compressed leaves. Disposing it releases the decompressed page the reader may point into.
    /// </summary>
    public readonly struct DecompressedValue(ValueReader reader, DecompressedLeafPage page) : IDisposable
    {
        public readonly ValueReader Reader = reader;
        public readonly DecompressedLeafPage Page = page;

        public void Dispose()
        {
            Page?.Dispose();
        }
    }

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
