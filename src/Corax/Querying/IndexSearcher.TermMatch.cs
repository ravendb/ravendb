using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Utils;
using Sparrow.Compression;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
#if DEBUG
#endif

namespace Corax.Querying;

public partial class IndexSearcher
{
    /// <summary>
    ///  Test API, should not be used anywhere else
    /// </summary>
    public TermMatch TermQuery(string field, string term, bool hasBoost = false) => TermQuery(FieldMetadataBuilder(field, hasBoost: hasBoost), term);
    public TermMatch TermQuery(Slice field, Slice term, bool hasBoost = false) => TermQuery(FieldMetadata.Build(field, default, default, default, default, hasBoost: hasBoost), term);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long GetContainerIdOfNumericalTerm<TNumeric>(in FieldMetadata field, out FieldMetadata numericalField, TNumeric term)
    {
        long containerId = -1;
        numericalField = default;
        if (typeof(TNumeric) == typeof(long))
        {
            numericalField = field.GetNumericFieldMetadata<long>(_transaction.Allocator);
            _fieldsTree
                ?.LookupFor<Int64LookupKey>(numericalField.FieldName)
                ?.TryGetValue((long)(object)term, out containerId);

        }
        else if (typeof(TNumeric) == typeof(double))
        {
            numericalField = field.GetNumericFieldMetadata<double>(_transaction.Allocator);
             _fieldsTree
                 ?.LookupFor<DoubleLookupKey>(numericalField.FieldName)
                ?.TryGetValue((double)(object)term, out containerId);
        }

        return containerId;
    }
    
    //Numerical TermMatch.
    public TermMatch TermQuery<TNumeric>(in FieldMetadata field, TNumeric term, CompactTree termsTree = null)
    {
        var containerId = GetContainerIdOfNumericalTerm(field, out var numericalField, term);

        return containerId == -1 
            ? TermMatch.CreateEmpty(this, Allocator) 
            : TermQuery(numericalField, containerId, 1);
    }
    
    public CompactTree GetTermsFor(Slice name) =>_fieldsTree?.CompactTreeFor(name); 
    
    public Lookup<Int64LookupKey> GetLongTermsFor(Slice name) =>_fieldsTree?.LookupFor<Int64LookupKey>(name);
    
    public Lookup<DoubleLookupKey> GetDoubleTermsFor(Slice name) =>_fieldsTree?.LookupFor<DoubleLookupKey>(name);

    public TermMatch TermQuery(in FieldMetadata field, string term, CompactTree termsTree = null)
    {
        return TryGetTermId(field, term, out var termId, out var termRatioToWholeCollection, termsTree)
            ? TermQuery(field, termId, termRatioToWholeCollection)
            : TermMatch.CreateEmpty(this, Allocator);
    }

    public TermMatch TermQuery(in FieldMetadata field, Slice term, CompactTree termsTree = null)
    {
        return TryGetTermId(field, term, out var termId, out var termRatioToWholeCollection, termsTree)
            ? TermQuery(field, termId, termRatioToWholeCollection)
            : TermMatch.CreateEmpty(this, Allocator);
    }

    public TermMatch TermQuery(in FieldMetadata field, CompactKey term, CompactTree tree)
    {
        if (TryGetTermId(field, term, tree, out var termId, out var termRatioToWholeCollection) == false)
            return TermMatch.CreateEmpty(this, Allocator);

        var matches = TermQuery(field, termId, termRatioToWholeCollection);

        #if DEBUG
        matches.Term = Encoding.UTF8.GetString(term.Decoded());
        #endif
        return matches;
    }

    internal bool TryGetTermId(in FieldMetadata field, string term, out long termId, out double termRatioToWholeCollection, CompactTree termsTree = null)
    {
        termId = -1;
        termRatioToWholeCollection = 1;
        var terms = termsTree ?? _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null && term != null)
        {
            // If either the term or the field does not exist the request will be empty.
            return false;
        }

        if (term is null || ReferenceEquals(term, Constants.ProjectionNullValue))
            return TryGetPostingListForNull(field, out termId);

        var termSlice = term switch
        {
            Constants.EmptyString => Constants.EmptyStringSlice,
            _ => EncodeAndApplyAnalyzer(field, term)
        };

        if (termSlice.Size == 0)
            return false;

        using var termKeyScope = new CompactKeyCacheScope(_fieldsTree.Llt);
        var termKey = termKeyScope.Key;
        termKey.Set(termSlice.AsReadOnlySpan());
        return TryGetTermId(field, termKey, terms, out termId, out termRatioToWholeCollection);
    }

    internal bool TryGetTermId(in FieldMetadata field, Slice term, out long termId, out double termRatioToWholeCollection, CompactTree termsTree = null)
    {
        termId = -1;
        termRatioToWholeCollection = 1;
        var terms = termsTree ?? _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty.
            return false;
        }

        if (term.Size == 0)
            return TryGetTermId(field, (CompactKey)null, terms, out termId, out termRatioToWholeCollection);

        using var termKeyScope = new CompactKeyCacheScope(_fieldsTree.Llt);
        var termKey = termKeyScope.Key;
        termKey.Set(term.AsReadOnlySpan());
        return TryGetTermId(field, termKey, terms, out termId, out termRatioToWholeCollection);
    }

    internal bool TryGetTermId(in FieldMetadata field, CompactKey term, CompactTree tree, out long termId, out double termRatioToWholeCollection)
    {
        termRatioToWholeCollection = 1;
        if (tree.TryGetValue(term, out termId) == false)
            return false;

        // Calculate bias for BM25 only when needed. There is no reason to calculate this in BM25 class because it would require to pass more information to primitive (and there is no reason to do so).
        if (field.HasBoost)
            termRatioToWholeCollection = GetTermRatioToWholeCollection(term, GetAverageTermLength(field, tree));

        return true;
    }

    /// <summary>
    /// Average byte length of the field's terms, 0 when it is unknown (the ratio is then 1).
    /// </summary>
    internal double GetAverageTermLength(in FieldMetadata field, CompactTree tree)
    {
        var totalTerms = tree.NumberOfEntries;
        long totalSum = totalTerms;
        if (_metadataTree.TryRead(field.TermLengthSumName, out var totalSumReader))
            totalSum = totalSumReader.ReadLittleEndianInt64();

        return totalTerms == 0 || totalSum == 0 ? 0 : totalSum / (double)totalTerms;
    }

    internal static double GetTermRatioToWholeCollection(CompactKey term, double averageTermLength)
    {
        return averageTermLength == 0 ? 1 : term.Decoded().Length / averageTermLength;
    }

    internal TermMatch TermQuery(in FieldMetadata field, long containerId, double termRatioToWholeCollection) => TermQuery(containerId, termRatioToWholeCollection, field.HasBoost);

    internal TermMatch TermQuery(long containerId, double termRatioToWholeCollection, bool isBoosting)
    {
        TermMatch matches;
        if ((containerId & (long)TermIdMask.PostingList) != 0)
        {
            var postingList = GetPostingList(containerId);
            matches = TermMatch.YieldSet(this, Allocator, postingList, termRatioToWholeCollection, isBoosting, IsAccelerated);
        }
        else if ((containerId & (long)TermIdMask.SmallPostingList) != 0)
        {
            var smallSetId = EntryIdEncodings.GetContainerId(containerId);
            Container.Get(_transaction.LowLevelTransaction, smallSetId, out var small);
            matches = TermMatch.YieldSmall(this, Allocator, small, termRatioToWholeCollection, isBoosting);
        }
        else
        {
            matches = TermMatch.YieldOnce(this, Allocator, containerId, termRatioToWholeCollection, isBoosting);
        }

        return matches;
    }

    public PostingList GetPostingList(long containerId)
    {
        var setId = EntryIdEncodings.GetContainerId(containerId);
        var setStateSpan = Container.GetReadOnly(_transaction.LowLevelTransaction, setId);

        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
        var set = new PostingList(_transaction.LowLevelTransaction, Slices.Empty, setState);
        return set;
    }

    public long NumberOfDocumentsUnderSpecificTerm<TData>(in FieldMetadata binding, TData term)
    {
        if (typeof(TData) == typeof(long))
        {
            var containerId = GetContainerIdOfNumericalTerm(binding, out var numericalField, (long)(object)term);
            return NumberOfDocumentsUnderSpecificTerm(containerId);
        }
        if (typeof(TData) == typeof(double))
        {
            var containerId = GetContainerIdOfNumericalTerm(binding, out var numericalField, (double)(object)term);
            return NumberOfDocumentsUnderSpecificTerm(containerId);
        }
            
        return NumberOfDocumentsUnderSpecificTerm(binding, (string)(object)term);
    }
    
    private long NumberOfDocumentsUnderSpecificTerm(in FieldMetadata binding, string term)
    {
        var terms = _fieldsTree?.CompactTreeFor(binding.FieldName);
        if (terms == null && term != null)
            return 0;
        
        if (term is null || ReferenceEquals(term, Constants.ProjectionNullValue))
        {
            var termMatch =  TryGetPostingListForNull(binding, out var postingListId) 
                ? TermQuery(binding, postingListId, 1D) 
                : TermMatch.CreateEmpty(this, Allocator);
            return termMatch.Count;
        }
        
        var termSlice = term switch
        {
            Constants.EmptyString => Constants.EmptyStringSlice,
            _ => EncodeAndApplyAnalyzer(binding, term)
        };
        
        return NumberOfDocumentsUnderSpecificTerm((CompactTree)terms, (Slice)termSlice);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long NumberOfDocumentsUnderSpecificTerm(CompactTree tree, Slice term)
    {
        var termAsSpan = term.AsReadOnlySpan();
        if (tree.TryGetValue(termAsSpan, out long containerId) == false)
        {
            if (termAsSpan.SequenceEqual(Constants.NullValueSpan))
            {
                if (TryGetPostingListForNull(tree.Name, out containerId, out _))
                    return NumberOfDocumentsUnderSpecificTerm(containerId);
            }
            
            return 0;
        }
        
        return NumberOfDocumentsUnderSpecificTerm(containerId);
    }
    
    private long NumberOfDocumentsUnderSpecificTerm(long containerId)
    {
        if (containerId == -1)
            return 0;
        
        if ((containerId & (long)TermIdMask.PostingList) != 0)
        {
            var setId = EntryIdEncodings.GetContainerId(containerId);
            var setStateSpan = Container.GetReadOnly(_transaction.LowLevelTransaction, setId);
            ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
            return setState.NumberOfEntries;
        }
        
        if ((containerId & (long)TermIdMask.SmallPostingList) != 0)
        {
            var smallSetId = EntryIdEncodings.GetContainerId(containerId);
            var small = Container.GetReadOnly(_transaction.LowLevelTransaction, smallSetId);
            var itemsCount = VariableSizeEncoding.Read<int>(small, out _);

            return itemsCount;
        }

        return 1;
    }
}
