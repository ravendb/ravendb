using System;
using Voron.Data.Compression;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe struct ReadMapEntryScope : IDisposable
    {
        public ReadMapEntryScope(DecompressedReadResult read)
        {
            _read = read;
            Data = PtrSize.Create(read.Reader.Base, read.Reader.Length);
        }

        public ReadMapEntryScope(PtrSize data)
        {
            Data = data;
            _read = null;
        }

        public readonly PtrSize Data;

        private readonly DecompressedReadResult _read;

        public void Dispose()
        {
            _read?.Dispose();
        }
    }
}
