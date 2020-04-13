using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron.Data.Fixed;
using Voron.Data.RawData;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.Tables
{
    public unsafe class TableValueCompressor
    {
        // we need at least 5 bytes to store the dictionary id (usually less)
        // we require a bit more because we want to ensure that we aren't wasting
        // resources by needlessly compressing data
        private const int OverheadSize = 32;
        private readonly TableValueBuilder _builder;

        public bool Compressed;
        public ByteString CompressedBuffer, RawBuffer;
        public ByteStringContext<ByteStringMemoryCache>.InternalScope CompressedScope, RawScope;
        public bool CompressionTried;

        public TableValueCompressor(TableValueBuilder builder)
        {
            _builder = builder;
        }

        public bool IsValid => RawBuffer.HasValue;

        public int Size
        {
            get
            {
                if (Compressed) return CompressedBuffer.Length;

                if (RawBuffer.HasValue) return RawBuffer.Length;

                return _builder.Size;
            }
        }

        public TableValueReader CreateReader(byte* pos)
        {
            return CompressedBuffer.HasValue ? new TableValueReader(RawBuffer.Ptr, RawBuffer.Length) : new TableValueReader(pos, RawBuffer.Length);
        }

        public bool TryCompression(Table table, TableSchema schema)
        {
            try
            {
                var tx = table._tx;
                int maxSpace = ZstdLib.GetMaxCompression(RawBuffer.Length);
                CompressedScope = tx.Allocator.Allocate(maxSpace + OverheadSize, out CompressedBuffer);
                Compressed = false;

                var compressionDictionary = tx.LowLevelTransaction.Environment.CompressionDictionariesHolder
                    .GetCompressionDictionaryFor(tx, table.CurrentCompressionDictionaryId);

                CompressionTried = true;
                var size = ZstdLib.Compress(RawBuffer.ToReadOnlySpan(), CompressedBuffer.ToSpan(), compressionDictionary);
                size += WriteVariableSizeIntInReverse(CompressedBuffer.Ptr + size, compressionDictionary.Id);
                CompressedBuffer.Truncate(size);
                var compressionRatio = GetCompressionRatio(size, RawBuffer.Length);
                if (compressionRatio > compressionDictionary.ExpectedCompressionRatio + 10)
                {
                    // training dictionaries is expensive, only do that if we see that the current compressed
                    // value is significantly worse than the previous one
                    var etagTree = table.GetFixedSizeTree(schema.CompressedEtagSourceIndex);
                    if(ShouldRetrain(etagTree))
                        MaybeTrainCompressionDictionary(table, etagTree);
                }

                if (CompressedBuffer.Length >= RawBuffer.Length)
                {
                    // we compressed too large, so we skip compression here
                    CompressedScope.Dispose();
                    // Explicitly not disposing this, we need to have the raw buffer
                    // when we do update then insert and the size is too large
                    // RawScope.Dispose();
                    Compressed = false;
                    return false;
                }

                Compressed = true;
                return true;
            }
            catch
            {
                CompressedScope.Dispose();
                RawScope.Dispose();
                throw;
            }
        }

        private bool ShouldRetrain(FixedSizeTree etagTree)
        {
            using (var it = etagTree.Iterate())
            {
                if (it.SeekToLast() == false)
                    return false;

                long lastEtag = it.CurrentKey;
                long total  = etagTree.NumberOfEntries;

                if(total < 16*1024)
                    return (lastEtag & 1024) == 0;
                if(total < 64 * 1024)
                    return (lastEtag & 2048) == 0;
                if(total < 256*1024)
                    return (lastEtag & 8192) == 0;
                if (total < 512 * 1024)
                    return (lastEtag & 16 * 1024) == 0;
                return (lastEtag & 32 * 1024) == 0;
            }
        }

        private byte GetCompressionRatio(int compressed, int raw)
        {
            return (byte)(compressed / (float)raw * 100);
        }

        public int WriteVariableSizeIntInReverse(byte* buffer, int value)
        {
            var count = 0;
            var v = (uint)value;
            while (v >= 0x80)
            {
                buffer[count++] = (byte)(v | 0x80);
                v >>= 7;
            }

            buffer[count++] = (byte)v;

            for (int i = count - 1; i >= count / 2; i--)
            {
                var tmp = buffer[i];
                buffer[i] = buffer[count - 1 - i];
                buffer[count - 1 - i] = tmp;
            }

            return count;
        }


        private void MaybeTrainCompressionDictionary(Table table, FixedSizeTree etagsTree)
        {
            // the idea is that we'll get better results by including the most recently modified documents
            // by iterating over the tag index, which is guaranteed to be always increasing
            var dataIds = new List<long>(256);
            var sizes = new UIntPtr[256];
            var totalSize = 0;
            int totalSkipped =0 ;
            using (var it = etagsTree.Iterate())
            {
                if (it.SeekToLast() == false)
                    return; // empty table, nothing to train on

                do
                {
                    long id = it.CreateReaderForCurrent().ReadLittleEndianInt64();
                    table.DirectRead(id, out var size);
                    if (size > 32 * 1024)
                    {
                        if (totalSkipped++ > 16 * 1024)
                            return;  // we are scanning too much, no need to try this hard
                        // we don't want to skip documents that are too big, they will compress
                        // well on their own, and likely be *too* unique to add meaningfully to the
                        // dictionary
                        continue; 
                    }

                    sizes[dataIds.Count] = (UIntPtr)size;
                    dataIds.Add(id);
                    totalSize += size;
                } while (dataIds.Count < 256 && it.MovePrev() && totalSize < 1024 * 1024);
            }

            if (dataIds.Count < 16)
                return;// too few samples to measure

            var tx = table._tx;
            using var _ = tx.Allocator.Allocate(totalSize, out var buffer);
            var cur = buffer.Ptr;
            foreach (long id in dataIds)
            {
                var ptr = table.DirectRead(id, out var size);
                Memory.Copy(cur, ptr, size);
                cur += size;
            }

            using var __ = tx.Allocator.Allocate(
                // the dictionary 
                Constants.Storage.PageSize - PageHeader.SizeOf - sizeof(CompressionDictionaryInfo)
                , out var dictionaryBuffer);

            Span<byte> dictionaryBufferSpan = dictionaryBuffer.ToSpan();
            ZstdLib.Train(new ReadOnlySpan<byte>(buffer.Ptr, totalSize), 
                new ReadOnlySpan<UIntPtr>(sizes, 0, dataIds.Count), 
                ref dictionaryBufferSpan);

            var dictionariesTree = tx.CreateTree(TableSchema.DictionariesSlice);

            var newId = (int)(dictionariesTree.State.NumberOfEntries + 1);

            using var compressionDictionary = new ZstdLib.CompressionDictionary(newId, dictionaryBuffer.Ptr, dictionaryBufferSpan.Length, 3);

            if (ShouldReplaceDictionary(tx, compressionDictionary) == false)
            {
                return;
            }

            table.CurrentCompressionDictionaryId = newId;
            compressionDictionary.ExpectedCompressionRatio = GetCompressionRatio(CompressedBuffer.Length, RawBuffer.Length);

            var rev = Bits.SwapBytes(newId);
            using var _____ = Slice.External(tx.Allocator, (byte*)&rev, sizeof(int), out var slice);
            using var ____ = dictionariesTree.DirectAdd(slice, sizeof(CompressionDictionaryInfo) + dictionaryBufferSpan.Length, out var dest);
            *((CompressionDictionaryInfo*)dest) = new CompressionDictionaryInfo
            {
                ExpectedCompressionRatio = compressionDictionary.ExpectedCompressionRatio
            };
            Memory.Copy(dest + sizeof(CompressionDictionaryInfo), dictionaryBuffer.Ptr, dictionaryBufferSpan.Length);

            tx.LowLevelTransaction.OnDispose += RecreateRecoveryDictionaries;
        }

        public static readonly byte[] EncryptionContext = Encoding.UTF8.GetBytes("Compress");

        private static void RecreateRecoveryDictionaries(IPagerLevelTransactionState obj)
        {
            if (!(obj is LowLevelTransaction llt) || llt.Committed == false)
                return; // we can't write on non committed transactions

            if (obj.Environment.Options is StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions)
                return; // no need for in mem mode

            lock (obj.Environment.CompressionDictionariesHolder)
            {
                using var tx = obj.Environment.ReadTransaction();

                var dictionaries = tx.ReadTree(TableSchema.DictionariesSlice);

                if (dictionaries == null)
                    return; // should never happen

                int nonceSize = (int)Sodium.crypto_stream_xchacha20_noncebytes();
                var subKeyLen = (int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
                int macSize = (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes();
                var nonceBuffer = stackalloc byte[nonceSize];
                var macBuffer = stackalloc byte[macSize];
                var subKey = stackalloc byte[subKeyLen];

                // for reliability's sake, we keep two copies of the compression
                // dictionaries
                for (int i = 0; i < 2; i++)
                {
                    var newPath = obj.Environment.Options.BasePath
                        .Combine(path: $"Compression{(i == 0 ? "A" : "B")}.Recovery")
                        .FullPath;

                    using var fs = File.Open(newPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
                    using var zip = new ZipArchive(fs,ZipArchiveMode.Update);
                    int lastWritten = 0;
                    if (zip.Entries.Count > 0)
                    {
                        // we try, if not successful, we'll start from scratch
                        int.TryParse(Path.GetFileNameWithoutExtension(zip.Entries[^1].Name), out lastWritten);
                    }

                    if (lastWritten == dictionaries.State.NumberOfEntries)
                        continue;

                    int rev = Bits.SwapBytes(lastWritten);
                    using var _ = Slice.External(tx.Allocator, (byte*)&rev, sizeof(int), out var key);
                    using var it = dictionaries.Iterate(true);
                    if (it.Seek(key) == false)
                        continue;

                    do
                    {
                        var dicId = it.CurrentKey.CreateReader().ReadBigEndianInt32();
                        var entry = zip.CreateEntry(dicId.ToString("D8")+ ".dic",
                            obj.Environment.Options.EncryptionEnabled ? CompressionLevel.NoCompression : CompressionLevel.Optimal
                            );
                        using var stream = entry.Open();
                        Span<byte> data = it.CreateReaderForCurrent().AsSpan();
                        if (obj.Environment.Options.EncryptionEnabled)
                        {
                            Sodium.randombytes_buf(nonceBuffer, Sodium.crypto_stream_xchacha20_noncebytes());
                            var nonceEntry = zip.CreateEntry(dicId.ToString("D8")+ ".nonce", CompressionLevel.NoCompression);
                            using var nonceStream = nonceEntry.Open();
                            nonceStream.Write(new ReadOnlySpan<byte>(nonceBuffer, nonceSize));

                            fixed (byte* pKey = obj.Environment.Options.MasterKey)
                            fixed (byte* d = data)
                            fixed (byte* ctx = EncryptionContext)
                            {
                                if (Sodium.crypto_kdf_derive_from_key(subKey, (UIntPtr)subKeyLen, (ulong)dicId, ctx, pKey) != 0)
                                    throw new InvalidOperationException("Unable to generate derived key");

                                ulong macLen = 0;
                                var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_encrypt_detached(
                                    d,
                                    macBuffer,
                                    &macLen,
                                    d,
                                    (ulong)data.Length,
                                    null,
                                    0,
                                    null,
                                    nonceBuffer,
                                    subKey);
                                if (rc != 0)
                                    throw new InvalidOperationException("Failed to encrypt dictionary");

                                var macEntry = zip.CreateEntry(dicId.ToString("D8")+ ".mac", CompressionLevel.NoCompression);
                                using var macStream = macEntry.Open();
                                macStream.Write(new ReadOnlySpan<byte>(macBuffer, (int)macLen));
                            }
                        }

                        stream.Write(data);
                    } while (it.MoveNext());

                }

            }

        }

        public bool ShouldReplaceDictionary(Transaction tx, ZstdLib.CompressionDictionary newDic)
        {
            int maxSpace = ZstdLib.GetMaxCompression(RawBuffer.Length) + OverheadSize;

            var newCompressBufferScope = tx.Allocator.Allocate(maxSpace, out var newCompressBuffer);
            try
            {
                var size = ZstdLib.Compress(RawBuffer.ToReadOnlySpan(), newCompressBuffer.ToSpan(), newDic);
                size += WriteVariableSizeIntInReverse(newCompressBuffer.Ptr + size, newDic.Id);
                newCompressBuffer.Truncate(size);

                // new dictionary is *really* bad for the current value
                if (size >= RawBuffer.Length ||
                    // we want to be conservative about changing dictionaries, we'll only replace it if there
                    // is a > 10% change in the data
                    size >= CompressedBuffer.Length * 0.9)
                {
                    // couldn't get better rate, abort and use the current one
                    newCompressBufferScope.Dispose();
                    return false;
                }

                Compressed = true;
               
                CompressedScope.Dispose();
                CompressedBuffer = newCompressBuffer;
                CompressedScope = newCompressBufferScope;
                return true;
            }
            catch
            {
                newCompressBufferScope.Dispose();
                throw;
            }
        }

        public void CopyTo(byte* ptr)
        {
            if (CompressedBuffer.HasValue)
            {
                CompressedBuffer.CopyTo(ptr);
                return;
            }

            RawBuffer.CopyTo(ptr);
        }

        public void DiscardCompressedData()
        {
            CompressionTried = false;
            Compressed = false;
            CompressedBuffer = default;
            CompressedScope.Dispose();
        }

        public void Reset()
        {
            RawBuffer = default;
            RawScope.Dispose();
            DiscardCompressedData();
        }
    }
}
