using System;

namespace Voron.Data.Compression
{
    public sealed class DecompressedReadResult(ValueReader reader, DecompressedLeafPage page) : ReadResult(reader), IDisposable
    {
        public void Dispose()
        {
            page?.Dispose();
        }
    }
}
