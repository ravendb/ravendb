using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Tryouts.Corax.Analyzers;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;
using Constants = Raven.Abstractions.Data.Constants;

namespace Tryouts.Corax
{
    public class FullTextIndex : IDisposable
    {
        private readonly StorageEnvironment _env;

        private readonly TableSchema _entriesSchema = new TableSchema()
            .DefineKey(new TableSchema.SchemaIndexDef
            {
                // TODO: this is standard b+tree, we probably want to use a fixed size tree here
                StartIndex = 0,
                Count = 1
            });

        private readonly UnmanagedBuffersPool _pool;
        private DefaultAnalyzer _analyzer;

        public FullTextIndex(StorageEnvironmentOptions options)
        {
            try
            {
                _pool = new UnmanagedBuffersPool("Index for " + options.BasePath);
                _env = new StorageEnvironment(options);
                _analyzer = new DefaultAnalyzer();
                using (var tx = _env.WriteTransaction())
                {
                    tx.CreateTree("Fields");
                    tx.CreateTree("Options");
                    _entriesSchema.Create(tx, "IndexEntries");
                    tx.Commit();
                }
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public Indexer CreateIndexer()
        {
            return new Indexer(this);
        }


        public Searcher CreateSearcher()
        {
            return new Searcher(this);
        }

        public class Searcher : IDisposable
        {
            private readonly FullTextIndex _parent;
            private readonly Transaction _tx;
            private readonly RavenOperationContext _context;

            public Searcher(FullTextIndex parent)
            {
                _parent = parent;
                _tx = _parent._env.ReadTransaction();
                _context = new RavenOperationContext(_parent._pool);
            }

            public unsafe string[] Query(string name, string value)
            {
                var property = _tx.ReadTree(name);
                if(property == null)
                    return Array.Empty<string>();

                var fixedSizeTree = new FixedSizeTree(_tx.LowLevelTransaction, property, value, 0);
                if (fixedSizeTree.NumberOfEntries == 0)
                    return Array.Empty<string>();

                var docs = new string[fixedSizeTree.NumberOfEntries];
                var entries = new Table(_parent._entriesSchema, "IndexEntries", _tx);

                long index = 0;
                long entryId = 0L;
                var key = new Slice((byte*)&entryId, sizeof(long));
                using (var it = fixedSizeTree.Iterate())
                {
                    entryId = IPAddress.HostToNetworkOrder(it.CurrentKey);

                    var tvr = entries.ReadByKey(key);
                    int size;
                    var ptr = tvr.Read(1, out size);
                    var entry = new BlittableJsonReaderObject(ptr, size, _context);
                    string docId;
                    entry.TryGet(Constants.DocumentIdFieldName, out docId);
                    docs[index] = docId;
                }

                return docs;
            }

            public void Dispose()
            {
                _tx?.Dispose();
                _context?.Dispose();
            }
        }

        public class Indexer : IDisposable
        {
            private readonly FullTextIndex _parent;
            private readonly RavenOperationContext _context;
            private readonly int _batchSize;
            private readonly List<BlittableJsonReaderObject> _newEntries = new List<BlittableJsonReaderObject>();
            private readonly List<string> _deletes = new List<string>();
            private long _size;
            private ITokenSource _tokenSource;


            public Indexer(FullTextIndex parent, int batchSize = 1024 * 1024 * 16)
            {
                _parent = parent;
                _batchSize = batchSize;
                _context = new RavenOperationContext(_parent._pool);
            }

            public void Delete(string identifier)
            {
                _deletes.Add(identifier);
            }

            public async Task NewEntry(DynamicJsonValue entry, string identifier)
            {
                entry[Constants.DocumentIdFieldName] = identifier;
                var blitEntry = await _context.ReadObjectWithExternalProperties(entry, identifier);


                for (int i = 0; i < blitEntry.Count; i++)
                {
                    var propertyByIndex = blitEntry.GetPropertyByIndex(i);
                    var fieldName = propertyByIndex.Item1;
                    _tokenSource = _parent._analyzer.CreateTokenSource(fieldName, _tokenSource);
                    _tokenSource.SetReader((LazyStringValue)propertyByIndex.Item2);
                    while (_tokenSource.Next())
                    {
                        var term = _tokenSource.GetCurrent();
                        _parent._analyzer.Process(fieldName, term);
                    }
                }

                _newEntries.Add(blitEntry);
                _size += blitEntry.Size;
                if (_size < _batchSize)
                    return;
                Flush();
            }

            private unsafe void Flush()
            {
                if (_newEntries.Count == 0 && _deletes.Count == 0)
                    return;

                using (var tx = _parent._env.WriteTransaction())
                {
                    var entries = new Table(_parent._entriesSchema, "IndexEntries", tx);

                    var identifiersTree = tx.CreateTree(Constants.DocumentIdFieldName);

                    foreach (var identifier in _deletes)
                    {
                        DeleteEntry(tx, entries, identifiersTree, identifier);
                    }

                    var options = tx.CreateTree("Options");

                    var readResult = options.Read("LastEntryId");
                    long lastEntryId = 1;
                    if (readResult != null)
                        lastEntryId = readResult.Reader.ReadLittleEndianInt64();

                    foreach (var entry in _newEntries)
                    {
                        AddEntry(tx, entries, entry, lastEntryId++);
                        entry.Dispose();
                    }
                    options.Add("LastEntryId", new Slice((byte*)&lastEntryId, sizeof(long)));
                    tx.Commit();
                }
                _size = 0;
                _newEntries.Clear();
            }

            private unsafe void DeleteEntry(Transaction tx, Table entries, Tree identifiersTree, string identifier)
            {
                // this should have just a single item in it
                var fixedSizeTree = new FixedSizeTree(tx.LowLevelTransaction, identifiersTree, identifier, 0);
                using (var it = fixedSizeTree.Iterate())
                {
                    if (it.Seek(long.MinValue) == false)
                        return;

                    var entryId = it.CurrentKey;
                    var bigEndianEntryId = IPAddress.HostToNetworkOrder(entryId);

                    var tvr = entries.ReadByKey(new Slice((byte*)&bigEndianEntryId, sizeof(long)));

                    int size;
                    var entry = new BlittableJsonReaderObject(tvr.Read(1, out size), size, _context);

                    for (int i = 0; i < entry.Count; i++)
                    {
                        var propertyByIndex = entry.GetPropertyByIndex(i);
                        var property = propertyByIndex.Item1;

                        //TODO: implement this without the field allocations
                        //var slice = new Slice(lazyStringValue.Buffer, (u short)lazyStringValue.Size);
                        var fieldTree = tx.CreateTree(property.ToString());

                        //TODO: right now only supporting strings
                        var value = (LazyStringValue)propertyByIndex.Item2;

                        var fst = new FixedSizeTree(tx.LowLevelTransaction, fieldTree,
                            new Slice(value.Buffer, (ushort)value.Size), 0);
                        fst.Delete(entryId);
                    }

                    entries.Delete(tvr.Id);
                }
            }

            private unsafe void AddEntry(Transaction tx, Table entries, BlittableJsonReaderObject entry, long entryId)
            {
                long bigEndianId = IPAddress.HostToNetworkOrder(entryId);
                entries.Set(new TableValueBuilder
                {
                    {(byte*)&bigEndianId, sizeof(long)},
                    {entry.BasePointer, entry.Size}
                });

                //for (int i = 0; i < entry.Count; i++)
                //{
                //    var propertyByIndex = entry.GetPropertyByIndex(i);
                //    var property = propertyByIndex.Item1;

                //    if (property.Size > byte.MaxValue)
                //        throw new InvalidOperationException("Field name cannot exceed 255 bytes");

                //    //TODO: implement this without the field allocations
                //    //var slice = new Slice(lazyStringValue.Buffer, (u short)lazyStringValue.Size);
                //    var fieldTree = tx.CreateTree(property.ToString());

                //    //TODO: right now only supporting strings
                //    var value = (LazyStringValue)propertyByIndex.Item2;
                //    if (value.Size > byte.MaxValue)
                //        throw new InvalidOperationException("Field value cannot exceed 255 bytes");

                //    var fst = new FixedSizeTree(tx.LowLevelTransaction, fieldTree,
                //        new Slice(value.Buffer, (ushort)value.Size), 0);
                //    fst.Add(entryId);
                //}
            }


            public void Dispose()
            {
                try
                {
                    Flush();
                }
                finally
                {
                    _context?.Dispose();
                }
            }
        }

        public void Dispose()
        {
            _pool?.Dispose();
            _env?.Dispose();
        }
    }
}