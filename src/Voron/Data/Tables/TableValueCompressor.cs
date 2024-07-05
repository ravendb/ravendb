using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Utils;
using Voron.Data.Fixed;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.Tables
{
    public sealed unsafe class TableValueCompressor
    {
        // we need at least 5 bytes to store the dictionary id (usually less)
        // we require a bit more because we want to ensure that we aren't wasting
        // resources by needlessly compressing data
        private const int OverheadSize = 32;

        internal const string CompressionRecoveryExtension = ".compression-recovery";
        public const string CompressionRecoveryExtensionGlob = "*" + CompressionRecoveryExtension;
        private readonly TableValueBuilder _builder;

        public bool Compressed;
        public ByteString CompressedBuffer, RawBuffer;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _compressedScope;
        public ByteStringContext<ByteStringMemoryCache>.InternalScope RawScope;
        public bool CompressionTried;

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<TableValueCompressor>("Compression");

        public TableValueCompressor(TableValueBuilder builder)
        {
            _builder = builder;
        }

        public bool IsValid => RawBuffer.HasValue;

        public int Size
        {
            get
            {
                if (Compressed)
                    return CompressedBuffer.Length;

                if (RawBuffer.HasValue)
                    return RawBuffer.Length;

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
                _compressedScope = tx.Allocator.Allocate(maxSpace + OverheadSize, out CompressedBuffer);
                Compressed = false;

                var compressionDictionary = tx.LowLevelTransaction.Environment.CompressionDictionariesHolder
                    .GetCompressionDictionaryFor(tx, table.CurrentCompressionDictionaryId);

                CompressionTried = true;
                var size = ZstdLib.Compress(RawBuffer.Ptr, RawBuffer.Length, CompressedBuffer.Ptr, CompressedBuffer.Length, compressionDictionary);
                size += WriteVariableSizeIntInReverse(CompressedBuffer.Ptr + size, compressionDictionary.Id);
                CompressedBuffer.Truncate(size);
                var compressionRatio = GetCompressionRatio(size, RawBuffer.Length);
                if (compressionRatio > compressionDictionary.ExpectedCompressionRatio + 10)
                {
                    // training dictionaries is expensive, only do that if we see that the current compressed
                    // value is significantly worse than the previous one
                    var etagTree = table.GetFixedSizeTree(schema.CompressedEtagSourceIndex);
                    if (ShouldRetrain(etagTree))
                        MaybeTrainCompressionDictionary(table, etagTree);
                }

                if (CompressedBuffer.Length >= RawBuffer.Length)
                {
                    // we compressed too large, so we skip compression here
                    _compressedScope.Dispose();
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
                _compressedScope.Dispose();
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

                return (lastEtag % 1024) == 0;
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
            var dataIds = ArrayPool<long>.Shared.Rent(256);
            var sizes = ArrayPool<UIntPtr>.Shared.Rent(256);
            try
            {
                int used = 0;
                var totalSize = 0;
                int totalSkipped = 0;

                using (var it = etagsTree.Iterate())
                {
                    if (it.SeekToLast() == false)
                        return; // empty table, nothing to train on

                    do
                    {
                        long id = it.CreateReaderForCurrent().Read<long>();
                        var size = table.GetSize(id);

                        if (size > 32 * 1024)
                        {
                            if (totalSkipped++ > 16 * 1024)
                                return;  // we are scanning too much, no need to try this hard
                                         // we want to skip documents that are too big, they will compress
                                         // well on their own, and likely be *too* unique to add meaningfully to the
                                         // dictionary
                            continue;
                        }

                        sizes[used] = (UIntPtr)size;
                        dataIds[used++] = id;
                        totalSize += size;
                    } while (used < 256 && it.MovePrev() && totalSize < 1024 * 1024);
                }

                if (used < 16)
                    return;// too few samples to measure

                var tx = table._tx;
                using (tx.Allocator.Allocate(totalSize, out var buffer))
                {
                    var cur = buffer.Ptr;
                    for (int i = 0; i < used; i++)
                    {
                        var ptr = table.DirectRead(dataIds[i], out int size);
                        Memory.Copy(cur, ptr, size);
                        cur += size;
                    }

                    using (tx.Allocator.Allocate(
                        // the dictionary
                        Constants.Storage.PageSize - PageHeader.SizeOf - sizeof(CompressionDictionaryInfo)
                        , out var dictionaryBuffer))
                    {
                        Span<byte> dictionaryBufferSpan = dictionaryBuffer.ToSpan();
                        ZstdLib.Train(new ReadOnlySpan<byte>(buffer.Ptr, totalSize),
                            new ReadOnlySpan<UIntPtr>(sizes, 0, used),
                            ref dictionaryBufferSpan);

                        var dictionariesTree = tx.CreateTree(TableSchema.CompressionDictionariesSlice);

                        var newId = (int)(dictionariesTree.State.Header.NumberOfEntries + 1);

                        using var compressionDictionary = new ZstdLib.CompressionDictionary(newId, dictionaryBuffer.Ptr, dictionaryBufferSpan.Length, 3);

                        if (ShouldReplaceDictionary(tx, compressionDictionary) == false)
                            return;

                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Compression dictionary '{newId}' was replaced in '{table.Name}' table.");

                        table.CurrentCompressionDictionaryId = newId;
                        compressionDictionary.ExpectedCompressionRatio = GetCompressionRatio(CompressedBuffer.Length, RawBuffer.Length);

                        var rev = Bits.SwapBytes(newId);
                        using (Slice.External(tx.Allocator, (byte*)&rev, sizeof(int), out var slice))
                        using (dictionariesTree.DirectAdd(slice, sizeof(CompressionDictionaryInfo) + dictionaryBufferSpan.Length, out var dest))
                        {
                            *((CompressionDictionaryInfo*)dest) =
                                new CompressionDictionaryInfo { ExpectedCompressionRatio = compressionDictionary.ExpectedCompressionRatio };
                            Memory.Copy(dest + sizeof(CompressionDictionaryInfo), dictionaryBuffer.Ptr, dictionaryBufferSpan.Length);
                        }

                        tx.LowLevelTransaction.OnDispose += RecreateRecoveryDictionaries;
                        tx.LowLevelTransaction.OnRollBack += state =>
                        {
                            if (state is not LowLevelTransaction llt)
                                return;
                            // ***************************************
                            // RavenDB-17758  - This is *important* - if we aren't committing the transaction, we *have*
                            // to remove the in memory cache for the dictionary, since it wasn't written to disk. If we
                            // keep it, we can compress data with a dictionary that we rolled back, and end up with data
                            // corruption!!!
                            // ****************************************
                            bool removed = llt.Environment.CompressionDictionariesHolder.Remove(newId);

                            if (Logger.IsInfoEnabled == false)
                                return;

                            Logger.Info(
                                removed
                                    ? $"Compression dictionary '{newId}' was removed during rollback in '{table.Name}' table."
                                    : $"Fail to remove compression dictionary '{newId}' during rollback in '{table.Name}' table.");
                        };
                    }
                }
            }
            finally
            {
                ArrayPool<long>.Shared.Return(dataIds);
                ArrayPool<UIntPtr>.Shared.Return(sizes);
            }
        }

        public static readonly byte[] EncryptionContext = Encoding.UTF8.GetBytes("Compress");

        private static void RecreateRecoveryDictionaries(LowLevelTransaction obj)
        {
            if (obj is not { Committed: true } ||
                obj.Environment.Options is StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions)
                return;

            lock (obj.Environment.CompressionDictionariesHolder)
            {
                using var tx = obj.Environment.ReadTransaction();

                var dictionaries = tx.ReadTree(TableSchema.CompressionDictionariesSlice);

                if (dictionaries == null)
                {
                    Debug.Assert(dictionaries != null);
                    return; // should never happen
                }

                var nonceSize = (int)Sodium.crypto_stream_xchacha20_noncebytes();
                var subKeyLen = (int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
                var macSize = (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes();
                var nonceBuffer = stackalloc byte[nonceSize];
                var macBuffer = stackalloc byte[macSize];
                var subKey = stackalloc byte[subKeyLen];

                // For reliability's sake, we keep two copies of the compression dictionaries
                for (int i = 0; i < 2; i++)
                {
                    string filename = $"Dictionary{(i == 0 ? "A" : "B")}";
                    var path = obj.Environment.Options.BasePath
                        .Combine(path: $"{filename}{CompressionRecoveryExtension}")
                        .FullPath;

                    try
                    {
                        using (var finalFileStream = File.Open(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read))
                        using (var zip = new ZipArchive(finalFileStream, ZipArchiveMode.Update))
                        {
                            int lastWritten = 0;

                            if (zip.Entries.Count > 0)
                                // Entries were added to the file from storage, which is in chronological order,
                                // ensuring that the last entry is the most recent.
                                lastWritten = int.Parse(Path.GetFileNameWithoutExtension(zip.Entries[^1].Name));

                            Debug.Assert(lastWritten <= dictionaries.State.Header.NumberOfEntries,
                                message: "The number of last written entry in recovery file must be equal to or less than the total number of entries in the state. " +
                                         "Any deviation from this is a bug.");

                            if (lastWritten == dictionaries.State.Header.NumberOfEntries)
                                continue;

                            AppendNewDictionaryEntries(lastWritten, zip);
                        }
                    }
                    catch (Exception innerEx)
                    {
                        if (Logger.IsOperationsEnabled)
                            Logger.Operations(msg: $"An unexpected error occurred while attempting to read the archive '{path}'. " +
                                                   $"The file will be recreated from scratch.", innerEx);
                        try
                        {
                            File.Delete(path);
                            using (var finalFileStream = File.Open(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read))
                            using (var zip = new ZipArchive(finalFileStream, ZipArchiveMode.Update))
                                AppendNewDictionaryEntries(lastWritten: 0, zip);
                        }
                        catch (Exception e)
                        {
                            var aggregateException = new AggregateException(e, innerEx);

                            if (Logger.IsOperationsEnabled)
                                Logger.Operations($"An unexpected error occurred while attempting to recreate recovery dictionaries to file '{path}'.",
                                    aggregateException);

                            throw aggregateException;
                        }
                    }
                }

                return;

                void AppendNewDictionaryEntries(int lastWritten, ZipArchive zip)
                {
                    // 1. Compression dictionary numbering starts from 1.
                    // 2. When appending new dictionaries to existing ones, start with the NEXT number following the existing ones.
                    int rev = Bits.SwapBytes(lastWritten + 1);

                    using ByteStringContext.ExternalScope _ = Slice.External(tx.Allocator, (byte*)&rev, sizeof(int), out var key);
                    using var it = dictionaries.Iterate(true);

                    if (it.Seek(key) == false)
                        return;

                    do
                    {
                        var dicId = it.CurrentKey.CreateReader().ReadBigEndian<int>();
                        var entry = zip.CreateEntry($"{dicId:D8}.dic",
                            obj.Environment.Options.Encryption.IsEnabled ? CompressionLevel.NoCompression : CompressionLevel.Optimal);

                        using var stream = entry.Open();

                        Span<byte> data = it.CreateReaderForCurrent().AsSpan();
                        if (obj.Environment.Options.Encryption.IsEnabled)
                            EncryptDictionary(dicId, data, zip);

                        stream.Write(data.ToArray(), 0, data.Length);
                    } while (it.MoveNext());
                }

                void EncryptDictionary(int dicId, Span<byte> data, ZipArchive zip)
                {
                    Sodium.randombytes_buf(nonceBuffer, (UIntPtr)nonceSize);
                    var nonceEntry = zip.CreateEntry($"{dicId:D8}.nonce", CompressionLevel.NoCompression);
                    using var nonceStream = nonceEntry.Open();
                    nonceStream.Write(new ReadOnlySpan<byte>(nonceBuffer, nonceSize));

                    fixed (byte* pKey = obj.Environment.Options.Encryption.MasterKey)
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

                        var macEntry = zip.CreateEntry($"{dicId:D8}.mac", CompressionLevel.NoCompression);
                        using var macStream = macEntry.Open();
                        macStream.Write(new ReadOnlySpan<byte>(macBuffer, (int)macLen));
                    }
                }
            }
        }

        internal bool ShouldReplaceDictionary(Transaction tx, ZstdLib.CompressionDictionary newDic)
        {
            int maxSpace = ZstdLib.GetMaxCompression(RawBuffer.Length) + OverheadSize;

            var newCompressBufferScope = tx.Allocator.Allocate(maxSpace, out var newCompressBuffer);
            try
            {
                var size = ZstdLib.Compress(RawBuffer.Ptr, RawBuffer.Length, newCompressBuffer.Ptr, newCompressBuffer.Length, newDic);
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

                _compressedScope.Dispose();
                CompressedBuffer = newCompressBuffer;
                _compressedScope = newCompressBufferScope;
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
            _compressedScope.Dispose();
        }

        public void Reset()
        {
            RawBuffer = default;
            RawScope.Dispose();
            DiscardCompressedData();
        }
    }
}
