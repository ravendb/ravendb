using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Sparrow.Platform;
using Sparrow.Server;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexDefinitionBaseServerSide
    {
        protected IndexDefinitionBaseServerSide()
        {
            ClusterState = new IndexDefinitionClusterState();
        }

        public string Name { get; protected set; }

        public abstract long Version { get; }

        public HashSet<string> Collections { get; protected set; }

        public IndexLockMode LockMode { get; set; }

        public IndexPriority Priority { get; set; }

        public IndexState State { get; set; }

        public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }

        internal IndexDefinitionClusterState ClusterState;

        public IndexDeploymentMode DeploymentMode { get; set; }

        public virtual bool HasDynamicFields => false;

        public virtual bool HasCompareExchange => false;

        public void Rename(string name, TransactionOperationContext context, StorageEnvironmentOptions options)
        {
            Name = name;
            Persist(context, options);
        }

        public abstract void Persist(TransactionOperationContext context, StorageEnvironmentOptions options);

        protected abstract void PersistMapFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer);

        public static readonly byte[] EncryptionContext = Encoding.UTF8.GetBytes("Indexes!");

        public static string GetIndexNameSafeForFileSystem(string name)
        {
            foreach (var invalidPathChar in Path.GetInvalidFileNameChars())
            {
                name = name.Replace(invalidPathChar, '_');
            }

            if (name.Length < 64)
                return name;
            // RavenDB-8220 To avoid giving the same path to indexes with the
            // same 64 chars prefix, we hash the full name. Note that this is
            // a persistent value and should NOT be changed.
            return name.Substring(0, 64) + "." + Hashing.XXHash32.Calculate(name);
        }

        public void Persist(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(Name));
            writer.WriteString(Name);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Version));
            writer.WriteInteger(Version);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Collections));
            writer.WriteStartArray();
            var isFirst = true;
            foreach (var collection in Collections)
            {
                if (isFirst == false)
                    writer.WriteComma();

                isFirst = false;
                writer.WriteString(collection);
            }

            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(nameof(LockMode));
            writer.WriteInteger((int)LockMode);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Priority));
            writer.WriteInteger((int)Priority);
            writer.WriteComma();

            writer.WritePropertyName(nameof(State));
            writer.WriteInteger((int)State);
            writer.WriteComma();

            writer.WritePropertyName(nameof(ArchivedDataProcessingBehavior));
            
            if (ArchivedDataProcessingBehavior == null)
                writer.WriteNull();
            else
                writer.WriteInteger((int)ArchivedDataProcessingBehavior);
            
            writer.WriteComma();

            PersistFields(context, writer);

            writer.WriteEndObject();
        }

        public bool ContainsField(string name)
        {
            return MapFields.ContainsKey(name);
        }

        internal abstract void Reset();

        protected abstract void PersistFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer);

        protected internal abstract IndexDefinition GetOrCreateIndexDefinitionInternal();

        public abstract IndexDefinitionCompareDifferences Compare(IndexDefinitionBaseServerSide indexDefinition);

        public abstract IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition);

        public Dictionary<string, IndexFieldBase> MapFields { get; protected set; }

        public Dictionary<string, IndexField> IndexFields { get; set; }

        public static class IndexVersion
        {
            public const long BaseVersion = 40_000;

            public const long TimeTicks = 50_000;

            public const long Analyzers = 52_000;
            
            public const long ReduceKeyProcessorHashDoubleFix = 52_001; // RavenDB-17572

            public const long ProperlyParseThreeDigitsMillisecondsDates = 52_002; // RavenDB-17711

            public const long EngineTypeStored = 54_000; // introducing Corax, added engine type to the index storage
            
            public const long GuaranteedOrderOfPropertiesInMapReduceIndexes_Legacy = 54_001; // RavenDB-17312
            
            public const long GuaranteedOrderOfGroupByFieldsInMapReduceIndexes = 54_002; // RavenDB-17312 - version 54_001 had an issue so we had to fix it and bump version again

            private const long TimeTicksSupportInJavaScriptIndexes_54 = 54_003; // RavenDB-19625

            public const long Base60Version = 60_000;

            public const long ProperlyParseDictionaryToStoredField = 60_000; // RavenDB-19560

            public const long TimeTicksSupportInJavaScriptIndexes_60 = 60_001; // RavenDB-19625

            public const long PhraseQuerySupportInCoraxIndexes = 60_002;
            public const long StoreOnlySupportInCoraxIndexes = 60_003; // RavenDB-22369
            public const long JavaScriptProperlyHandleDynamicFieldsIndexFields = 60_004; //RavenDB-22363
            public const long UseNonExistingPostingList = 60_005; // RavenDB-22703

            /// <summary>
            /// Remember to bump this
            /// </summary>
            public const long CurrentVersion = UseNonExistingPostingList;

            public static bool IsTimeTicksInJavaScriptIndexesSupported(long indexVersion)
            {
                if (indexVersion >= Base60Version)
                {
                    return indexVersion >= TimeTicksSupportInJavaScriptIndexes_60;
                }

                return indexVersion >= TimeTicksSupportInJavaScriptIndexes_54;
            }
        }
    }

    public abstract class IndexDefinitionBaseServerSide<T> : IndexDefinitionBaseServerSide where T : IndexFieldBase
    {
        protected const string MetadataFileName = "metadata";

        protected static readonly Slice DefinitionSlice;
        private long _indexVersion;
        private int? _cachedHashCode;

        internal IndexDefinitionBaseServerSide(
            string name,
            IEnumerable<string> collections,
            IndexLockMode lockMode,
            IndexPriority priority,
            IndexState state,
            T[] mapFields,
            long indexVersion,
            IndexDeploymentMode? deploymentMode,
            IndexDefinitionClusterState clusterState,
            ArchivedDataProcessingBehavior? archivedDataProcessingBehavior
            )
        {
            Name = name;
            DeploymentMode = deploymentMode ?? IndexDeploymentMode.Parallel;
            ArchivedDataProcessingBehavior = archivedDataProcessingBehavior;
            Collections = new HashSet<string>(collections, StringComparer.OrdinalIgnoreCase);

            MapFields = new Dictionary<string, IndexFieldBase>(StringComparer.Ordinal);
            IndexFields = new Dictionary<string, IndexField>(StringComparer.Ordinal);
            var lastUsedId = new Reference<int>() { Value = mapFields.Length };


            foreach (var field in mapFields)
            {
                MapFields.Add(field.Name, field);

                if (field is AutoIndexField autoField)
                {
                    foreach (var indexField in autoField.ToIndexFields(lastUsedId))
                    {
                        IndexFields.Add(indexField.Name, indexField);
                    }
                }
                else if (field is IndexField indexField)
                    IndexFields.Add(indexField.Name, indexField);
            }

            LockMode = lockMode;
            Priority = priority;
            State = state;
            _indexVersion = indexVersion;
            ClusterState.LastIndex = clusterState?.LastIndex ?? 0;
            ClusterState.LastStateIndex = clusterState?.LastStateIndex ?? 0;
            ClusterState.LastRollingDeploymentIndex = clusterState?.LastRollingDeploymentIndex ?? 0;
        }

        static IndexDefinitionBaseServerSide()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Definition", ByteStringType.Immutable, out DefinitionSlice);
            }
        }

        public override long Version => _indexVersion;

        internal override void Reset()
        {
            _indexVersion = IndexVersion.CurrentVersion;
        }

        public override void Persist(TransactionOperationContext context, StorageEnvironmentOptions options)
        {
            var tree = context.Transaction.InnerTransaction.CreateTree("Definition");
            using (var stream = new MemoryStream())
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                Persist(context, writer);

                writer.Flush();

                stream.Position = 0;

                if (options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                {
                    using (var metadata = File.Open(options.BasePath.Combine(MetadataFileName).FullPath, FileMode.Create))
                    {
                        if (options.Encryption.IsEnabled)
                        {
                            EncryptStream(options, stream);
                        }

                        stream.CopyTo(metadata);
                        stream.Position = 0;
                    }
                }

                using (Slice.From(context.Allocator, stream.ToArray(), out Slice val))
                {
                    tree.Add(DefinitionSlice, val);
                }
            }
        }

        private static unsafe void EncryptStream(StorageEnvironmentOptions options, MemoryStream stream)
        {
            var data = stream.ToArray();
            var nonce = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes()); // 192-bit
            var encryptedData = new byte[data.Length + (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes()]; // data length + 128-bit mac

            fixed (byte* ctx = EncryptionContext)
            fixed (byte* pData = data)
            fixed (byte* pEncryptedData = encryptedData)
            fixed (byte* pNonce = nonce)
            fixed (byte* pKey = options.Encryption.MasterKey)
            {
                var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
                var subKey = stackalloc byte[(int)subKeyLen];

                if (Sodium.crypto_kdf_derive_from_key(subKey, subKeyLen, (ulong)SodiumSubKeyId.IndexDef, ctx, pKey) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");

                ulong cLen;
                var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_encrypt(
                    pEncryptedData,
                    &cLen,
                    pData,
                    (ulong)data.Length,
                    null,
                    0,
                    null,
                    pNonce,
                    subKey
                );

                Debug.Assert(cLen <= (ulong)data.Length + (ulong)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes());

                if (rc != 0)
                    throw new InvalidOperationException($"Unable to encrypt stream, rc={rc}");
            }

            // reset the stream and write the encrypted data to it
            stream.SetLength(0);
            stream.Write(encryptedData, 0, encryptedData.Length);
            stream.Write(nonce, 0, nonce.Length);
            stream.Position = 0;
        }

        protected static Stream GetIndexDefinitionStream(StorageEnvironment environment, Transaction tx)
        {
            var tree = tx.CreateTree("Definition");
            var result = tree.Read(DefinitionSlice);
            if (result == null)
                return null;

            var stream = result.Reader.AsStream();
            if (environment.Options.Encryption.IsEnabled)
            {
                using (stream)
                {
                    var ms = new MemoryStream();
                    result.Reader.AsStream().CopyTo(ms);
                    ms.Position = 0;
                    DecryptStream(environment.Options, ms);
                    return ms;
                }
            }
            return stream;
        }

        private static unsafe void DecryptStream(StorageEnvironmentOptions options, MemoryStream stream)
        {
            var buffer = stream.ToArray();
            var nonce = new byte[(int)Sodium.crypto_aead_xchacha20poly1305_ietf_npubbytes()];
            var data = new byte[buffer.Length - nonce.Length];

            Array.Copy(buffer, 0, data, 0, buffer.Length - nonce.Length);
            Array.Copy(buffer, buffer.Length - nonce.Length, nonce, 0, nonce.Length);

            var decryptedData = new byte[data.Length - (int)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes()];

            fixed (byte* ctx = EncryptionContext)
            fixed (byte* pData = data)
            fixed (byte* pDecryptedData = decryptedData)
            fixed (byte* pNonce = nonce)
            fixed (byte* pKey = options.Encryption.MasterKey)
            {
                var subKeyLen = Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes();
                var subKey = stackalloc byte[(int)subKeyLen];

                if (Sodium.crypto_kdf_derive_from_key(subKey, subKeyLen, (ulong)SodiumSubKeyId.IndexDef, ctx, pKey) != 0)
                    throw new InvalidOperationException("Unable to generate derived key");

                ulong mLen;
                var rc = Sodium.crypto_aead_xchacha20poly1305_ietf_decrypt(
                    pDecryptedData,
                    &mLen,
                    null,
                    pData,
                    (ulong)data.Length,
                    null,
                    0,
                    pNonce,
                    subKey
                );

                Debug.Assert(mLen <= (ulong)data.Length - (ulong)Sodium.crypto_aead_xchacha20poly1305_ietf_abytes());

                if (rc != 0)
                    throw new InvalidOperationException($"Unable to decrypt stream, rc={rc}");
            }

            // reset the stream and write the decrypted data to it
            stream.SetLength(0);
            stream.Write(decryptedData, 0, decryptedData.Length);
            stream.Position = 0;
        }

        public virtual bool TryGetField(string field, out T value)
        {
            if (MapFields.TryGetValue(field, out var mapField))
            {
                value = mapField.As<T>();

                return true;
            }

            value = null;
            return false;
        }

        public virtual T GetField(string field)
        {
            return MapFields[field].As<T>();
        }

        public override int GetHashCode()
        {
            if (_cachedHashCode != null)
                return _cachedHashCode.Value;

            unchecked
            {
                var hashCode = MapFields?.GetDictionaryHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Name?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Collections?.GetEnumerableHashCode() ?? 0);

                hashCode = ComputeRestOfHash(hashCode);

                _cachedHashCode = hashCode;

                return hashCode;
            }
        }

        protected abstract int ComputeRestOfHash(int hashCode);

        protected static string ReadName(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(nameof(Name), out string name) == false || String.IsNullOrWhiteSpace(name))
                throw new InvalidOperationException("No persisted name");

            return name;
        }

        protected static string[] ReadCollections(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(nameof(Collections), out BlittableJsonReaderArray jsonArray) == false || jsonArray.Length == 0)
                throw new InvalidOperationException("No persisted collections");

            var result = new string[jsonArray.Length];
            for (var i = 0; i < jsonArray.Length; i++)
                result[i] = jsonArray.GetStringByIndex(i);

            return result;
        }

        protected static IndexLockMode ReadLockMode(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(nameof(LockMode), out int lockModeAsInt) == false)
                throw new InvalidOperationException("No persisted lock mode");

            return (IndexLockMode)lockModeAsInt;
        }

        protected static IndexPriority ReadPriority(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(nameof(Priority), out int priorityAsInt) == false)
                throw new InvalidOperationException("No persisted priority");

            return (IndexPriority)priorityAsInt;
        }

        protected static IndexState ReadState(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(nameof(State), out int StateAsInt) == false)
                return IndexState.Normal;

            return (IndexState)StateAsInt;
        }

        protected static ArchivedDataProcessingBehavior? ReadArchivedDataProcessingBehavior(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(nameof(ArchivedDataProcessingBehavior), out int? archivedDataProcessingBehaviorAsInt) == false)
                return null;

            if (archivedDataProcessingBehaviorAsInt == null)
                return null;
            
            return (ArchivedDataProcessingBehavior)archivedDataProcessingBehaviorAsInt;
        }

        protected static long ReadVersion(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(nameof(Version), out long version) == false)
                return IndexVersion.BaseVersion;

            return version;
        }
    }
}
