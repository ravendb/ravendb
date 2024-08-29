using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Analyzers;
using Corax.Pipeline;
using Corax.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.Containers;
using Voron.Util;

namespace Corax.Indexing;

public partial class IndexWriter
{
    public sealed class IndexEntryBuilder :  IIndexEntryBuilder, IDisposable
    {
        private readonly IndexWriter _parent;
        private long _entryId;
        private int _termPerEntryIndex;
        public bool Active;
        private int _buildingList;
        private Slice _documentId;
        private bool _mapFinishedSuccessfully = false;

        public long EntryId => _entryId;

        public IndexEntryBuilder(IndexWriter parent)
        {
            _parent = parent;
        }

        public void Boost(float boost)
        {
            _parent.BoostEntry(_entryId, boost);
        }

        public void Init(long entryId, int termsPerEntryIndex, Slice documentId)
        {
            Active = true;
            _mapFinishedSuccessfully = false;
            _entryId = entryId;
            _termPerEntryIndex = termsPerEntryIndex;
            _documentId = documentId;
        }

        /// <summary>
        /// This method must be called before dispose to indicate there were no exceptions during indexing.
        /// If this is not called, we will start the rollback procedure (see more at Dispose()).
        /// This allows us to avoid using a try...catch...finally block, thereby avoiding the overhead associated with the catch block.
        /// </summary>
        public void EndWriting()
        {
            Debug.Assert(Active, "Active");
            _mapFinishedSuccessfully = true;
        }
        
        public void Dispose()
        {
            if (_mapFinishedSuccessfully == false)
            {
                // When we encounter an exception during the building process (from the converter),
                // we need to rollback the data we've already written to the in-memory mapping. 
                // For this particular scenario, we will use the mechanism we created for handling the Map-Reduce
                // scenario where we have indexing and removals of the same document in the same indexing batch.
                // We will commit all the data we have currently gathered and immediately call delete on the invalid document
                // (which have to be handled by caller to avoid multiple calls of delete),
                // This would allow us to retrieve all the terms it contains using the already built-in mechanisms.
                // We also have to ensure that we're writing the current document ID into the compact tree since
                // most writes include the ID at the end of the mapping (e.g., to avoid indexing empty documents).
                // For the fanout scenario, we will rollback the whole document.
                Write(Constants.IndexWriter.PrimaryKeyFieldId, _documentId);
            }
            Active = false;
        }

        public void WriteNull(int fieldId, string path)
        {
            var field = GetField(fieldId, path);
            if (field.ShouldStore)
            {
                RegisterEmptyOrNull(field, StoredFieldType.Null);
            }

            if (field.ShouldIndex)
                ExactInsert(field, Constants.NullValueSlice);
        }

        public void WriteNonExistingMarker(int fieldId, string path)
        {
            var field = GetField(fieldId, path);

            if (field.ShouldIndex)
                ExactInsert(field, Constants.NonExistingValueSlice);
        }

        private IndexedField GetField(int fieldId, string path)
        {
            var field = fieldId != Constants.IndexWriter.DynamicField
                ? _parent._knownFieldsTerms[fieldId]
                : _parent.GetDynamicIndexedField(_parent._entriesAllocator, path);
            return field;
        }

        void Insert(IndexedField field, ReadOnlySpan<byte> value)
        {
            if (field.Analyzer != null)
                AnalyzeInsert(field, value);
            else
                ExactInsert(field, value);
        }

        public ReadOnlySpan<byte> AnalyzeSingleTerm(int fieldId, ReadOnlySpan<byte> value)
        {
            var field = GetField(fieldId, null);
            AnalyzeTerm(field, value, field.Analyzer, out Span<byte> wordsBuffer, out Span<Token> tokens);
            if (tokens.Length == 0)
                return ReadOnlySpan<byte>.Empty;
            if (tokens.Length > 1)
                ThrowTooManyTokens(tokens, value);

            return wordsBuffer.Slice(tokens[0].Offset, (int)tokens[0].Length);


            void ThrowTooManyTokens(Span<Token> tokens, ReadOnlySpan<byte> v)
            {
                throw new InvalidOperationException("Expected to get a single token from term, but got: " + tokens.Length + ", tokens: " +
                                                    Encoding.UTF8.GetString(v));
            }
        }

        void AnalyzeInsert(IndexedField field, ReadOnlySpan<byte> value)
        {
            AnalyzeTerm(field, value, field.Analyzer, out Span<byte> wordsBuffer, out Span<Token> tokens);

            for (int i = 0; i < tokens.Length; i++)
            {
                ref var token = ref tokens[i];

                if (token.Offset + token.Length > _parent._encodingBufferHandler.Length)
                    _parent.ThrowInvalidTokenFoundOnBuffer(field, value, wordsBuffer, tokens, token);

                var word = new Span<byte>(_parent._encodingBufferHandler, token.Offset, (int)token.Length);
                ExactInsert(field, word);
            }
        }

        private void AnalyzeTerm(IndexedField field, ReadOnlySpan<byte> value, Analyzer analyzer, out Span<byte> wordsBuffer, out Span<Token> tokens)
        {
            if (value.Length > _parent._encodingBufferHandler.Length)
            {
                analyzer.GetOutputBuffersSize(value.Length, out var outputSize, out var tokenSize);
                if (outputSize > _parent._encodingBufferHandler.Length || tokenSize > _parent._tokensBufferHandler.Length)
                    _parent.UnlikelyGrowAnalyzerBuffer(outputSize, tokenSize);
            }

            wordsBuffer = _parent._encodingBufferHandler;
            tokens = _parent._tokensBufferHandler;
            analyzer.Execute(value, ref wordsBuffer, ref tokens, ref _parent._utf8ConverterBufferHandler);

            if (tokens.Length > 1)
            {
                field.HasMultipleTermsPerField = true;
            }
        }

        ref EntriesModifications ExactInsert(IndexedField field, ReadOnlySpan<byte> value)
        {
            Debug.Assert(field.FieldIndexingMode != FieldIndexingMode.No, "field.FieldIndexingMode != FieldIndexingMode.No");
            
            ByteStringContext<ByteStringMemoryCache>.InternalScope? scope = CreateNormalizedTerm(_parent._entriesAllocator, value, out var slice);

            // We are gonna try to get the reference if it exists, but we wont try to do the addition here, because to store in the
            // dictionary we need to close the slice as we are disposing it afterwards. 
            ref var termLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Textual, slice, out var exists);
            if (exists == false)
            {
                termLocation = field.Storage.Count;
                field.Storage.AddByRef(new EntriesModifications(value.Length));
                scope = null; // We don't want the fieldname (slice) to be returned.
            }

            if (_buildingList > 0)
            {
                field.HasMultipleTermsPerField = true;
            }

            ref var term = ref field.Storage.GetAsRef(termLocation);
            term.Addition(_parent._entriesAllocator, _entryId, _termPerEntryIndex, freq: 1);

            // Creates a mapping for PhraseQuery
            if (field.FieldSupportsPhraseQuery)
            {
                // We're aligning our EntryToTerms list to have exactly _termPerEntryIndex items.
                // For most use cases, we will append only one element for each document, but we may be in a situation when the difference between sizes is bigger than 1.
                // This happens when previously indexed documents have not inserted any term for our field (mostly occurs in AutoIndexes when a document has no 'Field'
                // and we do not set explicit null configuration). In such a situation, we have to fill our 'gap' with the default NativeList (where Count is 0)
                // since we rely on it during the creation of the mapping in the Commit phase.
                if (field.EntryToTerms.Count <= _termPerEntryIndex)
                {
                    var additionalItems = Math.Max(1, _termPerEntryIndex - field.EntryToTerms.Count + 1);
                    field.EntryToTerms.EnsureCapacityFor(_parent._entriesAllocator, additionalItems);
                    
                    for (var i = field.EntryToTerms.Count; i <= _termPerEntryIndex; i++)
                    {
                        var nativeList = new NativeList<int>();
                        
                        if (i == _termPerEntryIndex)
                            nativeList.Initialize(_parent._entriesAllocator, 1);
                        
                        field.EntryToTerms.AddByRefUnsafe() = nativeList;
                    }
                }
                
                
                ref var nativeEntryTerms = ref field.EntryToTerms[_termPerEntryIndex];
                if (nativeEntryTerms.TryAdd(termLocation) == false)
                {
                    nativeEntryTerms.Grow(_parent._entriesAllocator, 1);
                    nativeEntryTerms.AddUnsafe(termLocation);
                }
                
                if (nativeEntryTerms.Count >= Constants.IndexWriter.MaxSizeOfTermVectorList)
                    ThrowDocumentExceedsPossibleTermAmount(field);
            }
            
            if (field.HasSuggestions)
                _parent.AddSuggestions(field, slice);

            scope?.Dispose();

            return ref term;
        }

        void NumericInsert(IndexedField field, long lVal, double dVal)
        {
            // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
            ref var doublesTermsLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Doubles, dVal, out bool fieldDoublesExist);
            if (fieldDoublesExist == false)
            {
                doublesTermsLocation = field.Storage.Count;
                field.Storage.AddByRef(new EntriesModifications(sizeof(double)));
            }

            // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
            ref var longsTermsLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Longs, lVal, out bool fieldLongExist);
            if (fieldLongExist == false)
            {
                longsTermsLocation = field.Storage.Count;
                field.Storage.AddByRef(new EntriesModifications(sizeof(long)));
            }

            ref var doublesTerm = ref field.Storage.GetAsRef(doublesTermsLocation);
            doublesTerm.Addition(_parent._entriesAllocator, _entryId, _termPerEntryIndex, freq: 1);

            ref var longsTerm = ref field.Storage.GetAsRef(longsTermsLocation);
            longsTerm.Addition(_parent._entriesAllocator, _entryId, _termPerEntryIndex, freq: 1);
        }

        private void RecordSpatialPointForEntry(IndexedField field, (double Lat, double Lng) coords)
        {
            field.Spatial ??= new();
            ref var terms = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Spatial, _entryId, out var exists);
            if (exists == false)
            {
                terms = new IndexedField.SpatialEntry {Locations = new List<(double, double)>(), TermsPerEntryIndex = _termPerEntryIndex};
            }

            terms.Locations.Add(coords);
        }

        internal void Clean()
        {
        }

        public void Write(int fieldId, ReadOnlySpan<byte> value) => Write(fieldId, null, value);

        public void Write(int fieldId, string path, ReadOnlySpan<byte> value)
        {
            var field = GetField(fieldId, path);
            if (value.Length > 0)
            {
                if (field.ShouldStore)
                {
                    RegisterTerm(field, value, StoredFieldType.Term);
                }

                if (field.ShouldIndex)
                    Insert(field, value);
            }
            else
            {
                if (field.ShouldStore)
                {
                    RegisterEmptyOrNull(field, StoredFieldType.Empty);
                }

                if (field.ShouldIndex)
                    ExactInsert(field, Constants.EmptyStringSlice);
            }
        }

        public void Write(int fieldId, string path, string value)
        {
            using var _ = Slice.From(_parent._entriesAllocator, value, out var slice);
            Write(fieldId, path, slice);
        }

        public void Write(int fieldId, ReadOnlySpan<byte> value, long longValue, double dblValue) => Write(fieldId, null, value, longValue, dblValue);

        public void Write(int fieldId, string path, string value, long longValue, double dblValue)
        {
            using var _ = Slice.From(_parent._entriesAllocator, value, out var slice);
            Write(fieldId, path, slice, longValue, dblValue);
        }

        public void Write(int fieldId, string path, ReadOnlySpan<byte> value, long longValue, double dblValue)
        {
            var field = GetField(fieldId, path);

            if (field.ShouldStore)
            {
                RegisterTerm(field, value, StoredFieldType.Tuple | StoredFieldType.Term);
            }


            if (field.ShouldIndex)
            {
                ref var term = ref ExactInsert(field, value);
                term.Long = longValue;
                term.Double = dblValue;
                NumericInsert(field, longValue, dblValue);
            }
        }

        public void WriteSpatial(int fieldId, string path, CoraxSpatialPointEntry entry)
        {
            var field = GetField(fieldId, path);

            if (field.ShouldIndex == false)
                throw new InvalidOperationException($"Your spatial field '{field.Name}' has 'Indexing' set to 'No'. Spatial fields cannot be stored, so this field is useless because it cannot be searched or retrieved."); 
            
            RecordSpatialPointForEntry(field, (entry.Latitude, entry.Longitude));

            var maxLen = Encoding.UTF8.GetMaxByteCount(entry.Geohash.Length);
            using var _ = _parent._entriesAllocator.Allocate(maxLen, out var buffer);
            var len = Encoding.UTF8.GetBytes(entry.Geohash, buffer.ToSpan());
            for (int i = 1; i <= len; ++i)
            {
                ExactInsert(field, buffer.ToReadOnlySpan()[..i]);
            }
        }

        public void Store(BlittableJsonReaderObject storedValue)
        {
            var field = _parent._knownFieldsTerms[^1];
            if (storedValue.HasParent)
            {
                storedValue = storedValue.CloneOnTheSameContext();
            }

            RegisterTerm(field, storedValue.AsSpan(), StoredFieldType.Raw);
        }

        public void Store(int fieldId, string name, BlittableJsonReaderObject storedValue)
        {
            var field = GetField(fieldId, name);
            if (storedValue.HasParent)
            {
                storedValue = storedValue.CloneOnTheSameContext();
            }

            RegisterTerm(field, storedValue.AsSpan(), StoredFieldType.Raw);
        }


        void RegisterTerm(IndexedField field, ReadOnlySpan<byte> term, StoredFieldType type)
        {
            if (_buildingList > 0)
            {
                type |= StoredFieldType.List;
            }

            var termsPerEntrySpan = _parent._termsPerEntryId.ToSpan();
            ref var entryTerms = ref termsPerEntrySpan[_termPerEntryIndex];

            _parent.InitializeFieldRootPage(field);

            var termId = Container.Allocate(
                _parent._transaction.LowLevelTransaction,
                _parent._storedFieldsContainerId,
                term.Length, field.FieldRootPage,
                out Span<byte> space);
            term.CopyTo(space);

            var recordedTerm = RecordedTerm.CreateForStored(entryTerms, type, termId);
            
            if (entryTerms.TryAdd(recordedTerm) == false)
            {
                entryTerms.Grow(_parent._entriesAllocator, 1);
                entryTerms.AddUnsafe(recordedTerm);
            }
        }

        public void RegisterEmptyOrNull(int fieldId, string fieldName, StoredFieldType type)
        {
            var field = GetField(fieldId, fieldName);
            RegisterEmptyOrNull(field, type);
        }

        void RegisterEmptyOrNull(IndexedField field, StoredFieldType type)
        {
            ref var entryTerms = ref _parent.GetEntryTerms(_termPerEntryIndex);

            _parent.InitializeFieldRootPage(field);
            var recordedTerm = RecordedTerm.CreateForStored(entryTerms, type, field.FieldRootPage);
            
            if (entryTerms.TryAdd(recordedTerm) == false)
            {
                entryTerms.Grow(_parent._entriesAllocator, 1);
                entryTerms.AddUnsafe(recordedTerm);
            }
        }

        public void IncrementList()
        {
            _buildingList++;
        }

        public void DecrementList()
        {
            _buildingList--;
        }

        public int ResetList()
        {
            var old = _buildingList;
            _buildingList = 0;
            return old;
        }

        public void RestoreList(int old)
        {
            _buildingList = old;
        }

        private static void ThrowDocumentExceedsPossibleTermAmount(IndexedField field)
        {
            throw new NotSupportedException($"Field '{field.Name} exceeds the limit of terms. Search field can have up to {Constants.IndexWriter.MaxSizeOfTermVectorList} elements.");
        }
    }
}
