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
    
    public CompactTree GetTermsFor(Slice name) => _fieldsTree.CompactTreeFor(name); 
    
    public Lookup<Int64LookupKey> GetLongTermsFor(Slice name) =>_fieldsTree.LookupFor<Int64LookupKey>(name);
    
    public Lookup<DoubleLookupKey> GetDoubleTermsFor(Slice name) =>_fieldsTree.LookupFor<DoubleLookupKey>(name);

    public TermMatch TermQuery(in FieldMetadata field, string term, CompactTree termsTree = null)
    {
        if (termsTree == null)
        {
            // If either the term or the fields tree does not exist the request will be empty. 
            if (_fieldsTree == null)
                return TermMatch.CreateEmpty(this, Allocator);

            if (_fieldsTree.TryGetCompactTreeFor(field.FieldName, out termsTree) == false)
                return TermMatch.CreateEmpty(this, Allocator);
        }

        if (term is null || ReferenceEquals(term, Constants.ProjectionNullValue))
        {
            return TryGetPostingListForNull(field, out var postingListId) 
                ? TermQuery(field, postingListId, 1D) 
                : TermMatch.CreateEmpty(this, Allocator);
        }
        
        var termSlice = term switch
        {
            Constants.EmptyString => Constants.EmptyStringSlice,
            _ => EncodeAndApplyAnalyzer(field, term)
        };

        CompactKey termKey;
        if (termSlice.Size != 0)
        {
            termKey = _fieldsTree.Llt.AcquireCompactKey();
            termKey.Set(termSlice.AsReadOnlySpan());
        }
        else
        {
            termKey = null;
        }

        return termKey is null 
            ? TermMatch.CreateEmpty(this, Allocator) 
            : TermQuery(field, termKey, termsTree);
    }
    
    //Should be already analyzed...
    public TermMatch TermQuery(in FieldMetadata field, Slice term, CompactTree termsTree = null)
    {
        if (termsTree == null)
        {
            // If either the term or the fields tree does not exist the request will be empty. 
            if (_fieldsTree == null)
                return TermMatch.CreateEmpty(this, Allocator);

            if (_fieldsTree.TryGetCompactTreeFor(field.FieldName, out termsTree) == false)
                return TermMatch.CreateEmpty(this, Allocator);
        }

        CompactKey termKey;
        if (term.Size != 0)
        {
            termKey = _fieldsTree.Llt.AcquireCompactKey();
            termKey.Set(term.AsReadOnlySpan());
        }
        else
        {
            termKey = null;
        }

        return TermQuery(field, termKey, termsTree);
    }

    public TermMatch TermQuery(in FieldMetadata field, CompactKey term, CompactTree tree)
    {
        if (tree.TryGetValue(term, out var value) == false)
            return TermMatch.CreateEmpty(this, Allocator);

        // Calculate bias for BM25 only when needed. There is no reason to calculate this in BM25 class because it would require to pass more information to primitive (and there is no reason to do so).
        double termRatioToWholeCollection = 1;
        if (field.HasBoost)
        {
            termRatioToWholeCollection = GetTermRatioToWholeCollection(field, term, tree);
        }

        var matches = TermQuery(field, value, termRatioToWholeCollection);
        
        #if DEBUG
        matches.Term = Encoding.UTF8.GetString(term.Decoded());
        #endif
        return matches;
    }

    private double GetTermRatioToWholeCollection(in FieldMetadata field, CompactKey term, CompactTree tree)
    {
        double termRatioToWholeCollection;
        var totalTerms = tree.NumberOfEntries;
        var totalSum = _metadataTree.Read(field.TermLengthSumName)?.Reader.ReadLittleEndianInt64() ?? totalTerms;

        if (totalTerms == 0 || totalSum == 0)
            termRatioToWholeCollection = 1;
        else
            termRatioToWholeCollection = term.Decoded().Length / (totalSum / (double)totalTerms);
        return termRatioToWholeCollection;
    }

    internal TermMatch TermQuery(in FieldMetadata field, long containerId, double termRatioToWholeCollection)
    {
        TermMatch matches;
        if ((containerId & (long)TermIdMask.PostingList) != 0)
        {
            var postingList = GetPostingList(containerId);
            matches = TermMatch.YieldSet(this, Allocator, postingList, termRatioToWholeCollection, field.HasBoost, IsAccelerated);
        }
        else if ((containerId & (long)TermIdMask.SmallPostingList) != 0)
        {
            var smallSetId = EntryIdEncodings.GetContainerId(containerId);
            Container.Get(_transaction.LowLevelTransaction, smallSetId, out var small);
            matches = TermMatch.YieldSmall(this, Allocator, small, termRatioToWholeCollection, field.HasBoost);
        }
        else
        {
            matches = TermMatch.YieldOnce(this, Allocator, containerId, termRatioToWholeCollection, field.HasBoost);
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
        if (_fieldsTree == null)
            return 0;

        var exist = _fieldsTree.TryGetCompactTreeFor(binding.FieldName, out var terms);
        if (exist == false && term != null)
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
                if (TryGetPostingListForNull(tree.Name, out containerId))
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
