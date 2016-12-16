using System;
using Voron.Data.Compression;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class ReadMapEntryScope : IDisposable
    {
        public ReadMapEntryScope(PtrSize entry, DecompressedLeafPage page = null)
        {
            Data = entry;
            _page = page;
        }

        public readonly PtrSize Data;

        private readonly DecompressedLeafPage _page;

        public void Dispose()
        {
            _page?.Dispose();
        }
    }
}