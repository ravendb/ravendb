using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Text;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public sealed unsafe class MapReduceResultsStore : IDisposable
    {
        public static ReadOnlySpan<byte> ReduceTreePrefixBytes => "__raven/map-reduce/#reduce-tree-"u8;
        public static ReadOnlySpan<byte> NestedValuesPrefixBytes => "__raven/map-reduce/#nested-section-"u8;

        public static readonly string ReduceTreePrefix = Encoding.UTF8.GetString(ReduceTreePrefixBytes);
        public static readonly string NestedValuesPrefix = Encoding.UTF8.GetString(NestedValuesPrefixBytes);
        private const int MaxUInt64Digits = 20;

        private readonly ulong _reduceKeyHash;
        private readonly TransactionOperationContext _indexContext;
        private readonly MapReduceIndexingContext _mapReduceContext;
        private readonly Slice _nestedValueKey;
        private ByteStringContext.InternalScope _nestedValueKeyScope;
        private readonly Transaction _tx;

        private NestedMapResultsSection _nestedSection;
        private bool _initializedTree = false;

        public MapResultsStorageType Type { get; private set; }

        public Tree Tree;

        public HashSet<long> ModifiedPages { get; private set; }

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
                    _nestedValueKeyScope = CreateKey(indexContext.Allocator, NestedValuesPrefixBytes, reduceKeyHash, out _nestedValueKey);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        private static ByteStringContext.InternalScope CreateKey(ByteStringContext allocator, ReadOnlySpan<byte> prefix, ulong reduceKeyHash, out Slice key)
        {
            Span<byte> buffer = stackalloc byte[prefix.Length + MaxUInt64Digits];
            prefix.CopyTo(buffer);
            Utf8Formatter.TryFormat(reduceKeyHash, buffer[prefix.Length..], out int digits);

            var scope = allocator.From(buffer, prefix.Length + digits, ByteStringType.Immutable, out ByteString str);
            key = new Slice(str);
            return scope;
        }

        private void InitializeTree(bool create)
        {
            // not released, the transaction keeps the names of the trees it opened
            CreateKey(_tx.Allocator, ReduceTreePrefixBytes, _reduceKeyHash, out Slice treeName);

            var options = _tx.LowLevelTransaction.Environment.Options.RunningOn32Bits ? TreeFlags.None : TreeFlags.LeafsCompressed;
            Tree = create ? _tx.CreateTree(treeName, flags: options) : _tx.ReadTree(treeName);

            if (_initializedTree == false)
            {
                ModifiedPages = new HashSet<long>();

                Tree.PageModified += (page, flags) =>
                {
                    if ((flags & PageFlags.Overflow) == PageFlags.Overflow)
                        return;

                    ModifiedPages.Add(page);
                    _mapReduceContext.FreedPages.Remove(page);
                };

                Tree.PageFreed += (page, flags) =>
                {
                    if ((flags & PageFlags.Overflow) == PageFlags.Overflow)
                        return;

                    _mapReduceContext.FreedPages.Add(page);
                    ModifiedPages.Remove(page);
                };

                _initializedTree = true;
            }
        }

        public void Delete(long id)
        {
            id = Bits.SwapBytes(id);

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    Slice entrySlice;
                    using (Slice.External(_indexContext.Allocator, (byte*) &id, sizeof(long), out entrySlice))
                        Tree.Delete(entrySlice);
                    break;
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();

                    section.Delete(id);

                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        public void Add(long id, BlittableJsonReaderObject result)
        {
            id = Bits.SwapBytes(id);

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    Slice entrySlice;
                    
                    using (Slice.External(_indexContext.Allocator, (byte*) &id, sizeof(long), out entrySlice))
                    {
                        using (Tree.DirectAdd(entrySlice, result.Size, out byte* ptr))
                            result.CopyTo(ptr);
                    }

                    break;
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();

                    if (_mapReduceContext.ReducePhaseTree.ShouldGoToOverflowPage(_nestedSection.SizeAfterAdding(result)))
                    {
                        // would result in an overflow, that would be a space waste anyway, let's move to tree mode
                        MoveExistingResultsToTree(section);
                        Add(Bits.SwapBytes(id), result); // now re-add the value, revert id to its original value
                    }
                    else
                    {
                        section.Add(id, result);
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        public ReadMapEntryScope Get(long id)
        {
            id = Bits.SwapBytes(id);

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    Slice entrySlice;
                    using (Slice.External(_indexContext.Allocator, (byte*)&id, sizeof(long), out entrySlice))
                    {
                        if (Tree.IsLeafCompressionSupported)
                        {
                            if (Tree.TryReadDecompressed(entrySlice, out var read) == false)
                                throw new InvalidOperationException($"Could not find a map result with id '{id}' in '{Tree.Name}' tree");
                            return new ReadMapEntryScope(read);
                        }
                        else
                        {
                            if (Tree.TryRead(entrySlice, out var reader) == false)
                                throw new InvalidOperationException($"Could not find a map result with id '{id}' in '{Tree.Name}' tree");
                            return new ReadMapEntryScope(PtrSize.Create(reader.Base, reader.Length));
                        }
                    }
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();
                    return new ReadMapEntryScope(section.Get(id));
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        private void MoveExistingResultsToTree(NestedMapResultsSection section)
        {
            Type = MapResultsStorageType.Tree;

            var byteType = (byte)Type;

            using (Slice.External(_indexContext.Allocator, &byteType, sizeof(byte), out Slice val))
                _mapReduceContext.ResultsStoreTypes.Add((long)_reduceKeyHash, val);
            InitializeTree(create: true);

            section.MoveTo(Tree);
        }

        public NestedMapResultsSection GetNestedResultsSection(Tree tree = null)
        {
            if (_nestedSection != null)
                return _nestedSection;

            _nestedSection = new NestedMapResultsSection(_indexContext.Environment, tree ?? _mapReduceContext.ReducePhaseTree, _nestedValueKey);

            return _nestedSection;
        }

        public void Dispose()
        {
            _nestedValueKeyScope.Dispose();
        }
    }
}
