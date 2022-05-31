using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Server.Compression;
using Voron;
using Voron.Impl;
using Voron.Data.Containers;
using Sparrow;
using System.Runtime.Intrinsics.X86;
using Corax.Pipeline;
using Corax.Queries;
using Sparrow.Server;

namespace Corax;

public sealed unsafe partial class IndexSearcher : IDisposable
{
    private readonly Transaction _transaction;
    private readonly IndexFieldsMapping _fieldMapping;

    private Page _lastPage = default;

    /// <summary>
    /// When true no SIMD instruction will be used. Useful for checking that optimized algorithms behave in the same
    /// way than reference algorithms. 
    /// </summary>
    public bool ForceNonAccelerated { get; set; }

    public bool IsAccelerated => Avx2.IsSupported && !ForceNonAccelerated;

    public long NumberOfEntries => _transaction.LowLevelTransaction.RootObjects.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;

    internal ByteStringContext Allocator => _transaction.Allocator;

    internal Transaction Transaction => _transaction;


    private readonly bool _ownsTransaction;

    // The reason why we want to have the transaction open for us is so that we avoid having
    // to explicitly provide the index searcher with opening semantics and also every new
    // searcher becomes essentially a unit of work which makes reusing assets tracking more explicit.
    public IndexSearcher(StorageEnvironment environment, IndexFieldsMapping fieldsMapping = null)
    {
        _ownsTransaction = true;
        _transaction = environment.ReadTransaction();
        _fieldMapping = fieldsMapping ?? new IndexFieldsMapping(_transaction.Allocator);
    }

    public IndexSearcher(Transaction tx, IndexFieldsMapping fieldsMapping = null)
    {
        _ownsTransaction = false;
        _transaction = tx;
        _fieldMapping = fieldsMapping ?? new IndexFieldsMapping(_transaction.Allocator);
    }

    public UnmanagedSpan GetIndexEntryPointer(long id)
    {
        var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id);
        int size = ZigZagEncoding.Decode<int>(data.ToSpan(), out var len);
        return data.ToUnmanagedSpan().Slice(size + len);
    }

    public IndexEntryReader GetReaderFor(long id)
    {
        return GetReaderFor(_transaction, ref _lastPage, id);
    }

    public static IndexEntryReader GetReaderFor(Transaction transaction, ref Page page, long id)
    {
        var data = Container.MaybeGetFromSamePage(transaction.LowLevelTransaction, ref page, id).ToSpan();
        int size = ZigZagEncoding.Decode<int>(data, out var len);
        return new IndexEntryReader(data.Slice(size + len));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IndexEntryReader GetReaderAndIdentifyFor(long id, out string key)
    {
        var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id).ToSpan();
        int size = ZigZagEncoding.Decode<int>(data, out var len);
        key = Encoding.UTF8.GetString(data.Slice(len, size));
        return new(data.Slice(size + len));
    }

    public string GetIdentityFor(long id)
    {
        var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id).ToSpan();
        int size = ZigZagEncoding.Decode<int>(data, out var len);
        return Encoding.UTF8.GetString(data.Slice(len, size));
    }

    public UnmanagedSpan GetRawIdentityFor(long id)
    {
        var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id).ToUnmanagedSpan();
        int size = ZigZagEncoding.Decode<int>(data, out var len);
        return data.Slice(len, size);
    }

    [SkipLocalsInit]
    private Slice EncodeAndApplyAnalyzer(string term, int fieldId)
    {
        if (term is null)
            return default;

        if (term.Length == 0 || term == Constants.EmptyString)
            return Constants.EmptyStringSlice;
        
        if (term == Constants.NullValue)
            return Constants.NullValueSlice;

        var encoded = Encoding.UTF8.GetBytes(term);
        Slice termSlice;
        if (fieldId == Constants.IndexSearcher.NonAnalyzer)
        {
            Slice.From(Allocator, encoded, out termSlice);
            return termSlice;
        }

        Slice.From(Allocator, ApplyAnalyzer(encoded, fieldId), out termSlice);
        return termSlice;
    }

    //todo maciej: notice this is very inefficient. We need to improve it in future. 
    // Only for KeywordTokenizer
    [SkipLocalsInit]
    internal unsafe ReadOnlySpan<byte> ApplyAnalyzer(ReadOnlySpan<byte> originalTerm, int fieldId)
    {
        if (_fieldMapping.TryGetByFieldId(fieldId, out var binding) == false
            || binding.FieldIndexingMode is FieldIndexingMode.Exact or FieldIndexingMode.Search
            || binding.Analyzer is null)
        {
            return originalTerm;
        }

        var analyzer = binding.Analyzer!;
        analyzer.GetOutputBuffersSize(originalTerm.Length, out int outputSize, out int tokenSize);
        
        Debug.Assert(outputSize < 1024 * 1024, "Term size is too big for analyzer.");
        Debug.Assert(Unsafe.SizeOf<Token>() * tokenSize < 1024 * 1024, "Analyzer wants to create too much tokens.");
        
        Span<byte> encoded = new byte[outputSize];
        Token* tokensPtr = stackalloc Token[tokenSize];
        var tokens = new Span<Token>(tokensPtr, tokenSize);
        
        analyzer.Execute(originalTerm, ref encoded, ref tokens);
        Debug.Assert(tokens.Length == 1, $"{nameof(ApplyAnalyzer)} should create only 1 token as a result.");

        return encoded;
    }

    public AllEntriesMatch AllEntries()
    {
        return new AllEntriesMatch(_transaction);
    }
    
    public bool TryGetTermsOfField(string field, out ExistsTermProvider existsTermProvider)
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(field);
        
        if (terms == null)
        {
            existsTermProvider = default;
            return false;
        }
        
        existsTermProvider = new ExistsTermProvider(this, _transaction.Allocator, terms, field);
        return true;
    }

    public void Dispose()
    {
        if (_ownsTransaction)
            _transaction?.Dispose();
    }
}
