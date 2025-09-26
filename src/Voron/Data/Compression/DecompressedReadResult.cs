using System;

namespace Voron.Data.Compression
{
    /// <summary>
    /// A wrapper around <see cref="ReadResult"/> allowing scoping it with <see cref="IDisposable"/>.
    /// </summary>
    public sealed class DecompressedReadResult(ReadResult result, DecompressedLeafPage page) : IDisposable
    {
        public ValueReader Reader => result.Reader; 
        
        public void Dispose() => page?.Dispose();
    }
}
