using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class MapReduceResultsStore : IDisposable
    {
        private readonly ulong _reduceKeyHash;
        private readonly TransactionOperationContext _indexContext;
        private readonly MapReduceIndexingContext _mapReduceContext;
        private readonly Slice _nestedValueKey;
        private ByteStringContext.Scope _nestedValueKeyScope;
        private readonly Transaction _tx;

        private NestedMapResultsSection _nestedSection;

        public MapResultsStorageType Type { get; private set; }

        public Tree Tree;
        public HashSet<long> ModifiedPages;
        public HashSet<long> FreedPages;

        public MapReduceResultsStore(ulong reduceKeyHash, MapResultsStorageType type, TransactionOperationContext indexContext, MapReduceIndexingContext mapReduceContext, bool create)
        {
            _reduceKeyHash = reduceKeyHash;
            Type = type;
            _indexContext = indexContext;
            _mapReduceContext = mapReduceContext;
            _tx = indexContext.Transaction.InnerTransaction;

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    InitializeTree(create);
                    break;
                case MapResultsStorageType.Nested:
                    _nestedValueKeyScope = Slice.From(indexContext.Allocator, "#reduceValues-" + reduceKeyHash, ByteStringType.Immutable, out _nestedValueKey);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        private void InitializeTree(bool create)
        {
            //TODO: Need better way to handle tree names

            var treeName = "#reduceTree-" + _reduceKeyHash;
            Tree = create ? _tx.CreateTree(treeName) : _tx.ReadTree(treeName);

            ModifiedPages = new HashSet<long>();
            FreedPages = new HashSet<long>();

            Tree.PageModified += page => ModifiedPages.Add(page);
            Tree.PageFreed += page => FreedPages.Add(page);
        }

        public void Delete(long id)
        {
            var entryId = id;

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    Slice entrySlice;
                    using (Slice.External(_indexContext.Allocator, (byte*) &entryId, sizeof(long), out entrySlice))
                        Tree.Delete(entrySlice);
                    break;
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();

                    section.Delete(id);

                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
            
            _mapReduceContext.EntryDeleted(id);
        }

        public void Add(long id, BlittableJsonReaderObject result)
        {
            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    using (result)
                    {
                        Slice entrySlice;
                        using (Slice.External(_indexContext.Allocator, (byte*) &id, sizeof(long), out entrySlice))
                        {
                            var pos = Tree.DirectAdd(entrySlice, result.Size);
                            result.CopyTo(pos);
                        }
                    }

                    break;
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();

                    section.Add(id, result);

                    if (_mapReduceContext.MapEntries.ShouldGoToOverflowPage(_nestedSection.Size))
                    {
                        // would result in an overflow, that would be a space waste anyway, let's move to tree mode
                        MoveExistingResultsToTree(section);

                        _nestedSection.Dispose();
                        _nestedSection = null;

                        _mapReduceContext.MapEntries.Delete(_nestedValueKey);
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        private void MoveExistingResultsToTree(NestedMapResultsSection section)
        {
            Type = MapResultsStorageType.Tree;

            var byteType = (byte)Type;

            Slice val;
            using (Slice.External(_indexContext.Allocator, &byteType, sizeof(byte), out val))
                _mapReduceContext.ResultsStoreTypes.Add((long)_reduceKeyHash, val);
            InitializeTree(create: true);

            foreach (var mapResult in section.GetResults())
            {
                Add(mapResult.Key, mapResult.Value);
            }
        }

        public NestedMapResultsSection GetNestedResultsSection()
        {
            if (_nestedSection != null)
                return _nestedSection;

            var read = _mapReduceContext.MapEntries.Read(_nestedValueKey);

            _nestedSection = read == null ? new NestedMapResultsSection() : new NestedMapResultsSection(read.Reader.Base, read.Reader.Length, _indexContext);

            return _nestedSection;
        }

        public void FlushNestedValues()
        {
            if (_nestedSection.Size > 0)
            {
                var pos = _mapReduceContext.MapEntries.DirectAdd(_nestedValueKey, _nestedSection.Size);

                _nestedSection.CopyTo(pos);
            }
            else
            {
                _mapReduceContext.MapEntries.Delete(_nestedValueKey);
            }
        }

        public void Dispose()
        {
            _nestedValueKeyScope.Dispose();
            if (_nestedSection != null)
            {
                _nestedSection.Dispose();
                _nestedSection = null;
            }
        }
    }
}