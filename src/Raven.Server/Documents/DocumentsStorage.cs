using System;
using System.Text;
using Microsoft.Extensions.Configuration;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Util.Conversion;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents
{
    public unsafe class DocumentsStorage : IDisposable
    {
        public string DataDirectory;

        private readonly ILog _log;

        private StorageEnvironment _env;
        private readonly string _name;
        private readonly IConfigurationRoot _config;
        private readonly TableSchema _docsSchema = new TableSchema();

        public DocumentsStorage(string name, IConfigurationRoot config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            _name = name;
            _config = config;
            _log = LogManager.GetLogger(typeof(DocumentsStorage).FullName + "." + _name);

        }

        public void Initialize()
        {
            var runInMemory = _config.Get<bool>("run.in.memory");
            if (runInMemory == false)
            {
                DataDirectory = _config.Get<string>("system.path").ToFullPath();
            }
            if (_log.IsDebugEnabled)
            {
                _log.Debug("Starting to open document storage for {0}", (runInMemory ? "<memory>" : DataDirectory));
            }
            var options = runInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(DataDirectory);

            // The documents schema is as follows
            // 3 fields (lowered key, etag, key, document)
            options.SchemaVersion = 1;
            _docsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = true,
                Name = "Docs"
            });
            _docsSchema.DefineIndex("CollectionEtags", new TableSchema.SchemaIndexDef
            {
                StartIndex = 1,
                Count = 1,
                Size = sizeof(long),
                MultiValue = false,
                IsGlobal = false
            });
            _docsSchema.DefineIndex("AllDocsEtags", new TableSchema.SchemaIndexDef
            {
                StartIndex = 1,
                Count = 1,
                Size = sizeof(long),
                MultiValue = false,
                IsGlobal = true
            });
            try
            {
                _env = new StorageEnvironment(options);
                using (var tx = _env.WriteTransaction())
                {
                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                if (_log.IsWarnEnabled)
                {
                    _log.FatalException(
                        "Could not open server store for " + (runInMemory ? "<memory>" : DataDirectory), e);
                }
                options.Dispose();
                throw;
            }
        }

        //TODO: proper etag generation
        private long _lastEtag;

        public Document Get(RavenOperationContext context, string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put", nameof(context));

            // TODO: Avoid allocations in this case by using pre-existing buffers
            var loweredKey = key.ToLowerInvariant();
            var loweredKeyBytes = Encoding.UTF8.GetBytes(loweredKey);

            if (loweredKeyBytes.Length > 255)
                throw new ArgumentException(
                    $"Key cannot exceed 255 bytes, but the key was {loweredKeyBytes.Length} bytes. The invalid key is '{key}'.",
                    nameof(key));

            var table = new Table(_docsSchema, context.Transaction);

            var tvr = table.ReadByKey(new Slice(loweredKeyBytes));
            if (tvr == null)
                return null;

            var result = new Document();
            int size;
            var ptr = tvr.Read(2, out size);
            result.Key = Encoding.UTF8.GetString(ptr, size);
            ptr = tvr.Read(1, out size);
            result.Etag = EndianBitConverter.Big.ToInt64(ptr);
            result.Data = new BlittableJsonReaderObject(tvr.Read(3, out size), size, context);

            return result;
        }

        public long Put(RavenOperationContext context, string key, long? expectedEtag, BlittableJsonReaderObject document)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Argument is null or whitespace", nameof(key));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Put", nameof(context));

            BlittableJsonReaderObject metadata;
            string collectionName;
            if (document.TryGet(Constants.Metadata, out metadata) == false ||
                document.TryGet(Constants.RavenEntityName, out collectionName) == false)
            {
                collectionName = "<no-collection>";
            }

            // TODO: Avoid allocations in this case by using pre-existing buffers
            var loweredKey = key.ToLowerInvariant();
            var loweredKeyBytes = Encoding.UTF8.GetBytes(loweredKey);
            var keyBytes = Encoding.UTF8.GetBytes(key);
            if (loweredKeyBytes.Length > 255)
                throw new ArgumentException(
                    $"Key cannot exceed 255 bytes, but the key was {loweredKeyBytes.Length} bytes. The invalid key is '{key}'.",
                    nameof(key));

            var etagBytes = EndianBitConverter.Big.GetBytes(++_lastEtag);
            fixed (byte* keyPtr = keyBytes)
            fixed (byte* loweredKeyPtr = loweredKeyBytes)
            fixed (byte* etagBytesPtr = etagBytes)
            {
                var tbv = new TableValueBuilder
                {
                    {loweredKeyPtr, loweredKeyBytes.Length},
                    {etagBytesPtr, etagBytes.Length},
                    {keyPtr, keyBytes.Length },
                    {document.BasePointer, document.Size}
                };

                _docsSchema.Create(context.Transaction, collectionName);

                var table = new Table(_docsSchema, collectionName, context.Transaction);
                var oldValue = table.ReadByKey(new Slice(loweredKeyPtr, (ushort)loweredKeyBytes.Length));
                if (oldValue == null)
                {
                    if (expectedEtag != null && expectedEtag != 0)
                    {
                        throw new ConcurrencyException(
                            $"Document {key} does not exists, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");
                    }
                    table.Insert(tbv);
                }
                else
                {
                    int size;
                    byte* pOldEtag = oldValue.Read(1, out size);
                    var oldEtag = EndianBitConverter.Big.ToInt64(pOldEtag);
                    if (expectedEtag != null && oldEtag != expectedEtag)
                        throw new ConcurrencyException(
                            $"Document {key} has etag {oldEtag}, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.");
                    table.Update(oldValue.Id, tbv);
                }
            }
            return -1;
        }

        public StorageEnvironment Environment => _env;

        public void Dispose()
        {
            _env?.Dispose();
            _env = null;
        }
    }
}