using System;
using Voron.Data.Compression;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe struct ReadMapEntryScope : IDisposable
    {
        public ReadMapEntryScope(in DecompressedValue read)
        {
            _decompressedPage = read.Page;
            Data = PtrSize.Create(read.Reader.Base, read.Reader.Length);
        }

        public ReadMapEntryScope(PtrSize data)
        {
            Data = data;
            _decompressedPage = null;
        }

        public readonly PtrSize Data;

        private readonly DecompressedLeafPage _decompressedPage;

        public void Dispose()
        {
            _decompressedPage?.Dispose();
        }
    }
}
