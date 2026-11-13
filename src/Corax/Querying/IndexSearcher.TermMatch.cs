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
            if (_fieldsTree != null && _fieldsTree.TryGetLookupFor<Int64LookupKey>(numericalField.FieldName, out var longLookup))
                longLookup.TryGetValue((long)(object)term, out containerId);
        }
        else if (typeof(TNumeric) == typeof(double))
        {
            numericalField = field.GetNumericFieldMetadata<double>(_transaction.Allocator);
            if (_fieldsTree != null && _fieldsTree.TryGetLookupFor<DoubleLookupKey>(numericalField.FieldName, out var doubleLookup))
                doubleLookup.TryGetValue((double)(object)term, out containerId);
        }

        return containerId;
    }
    
    //Numerical TermMatch.
    public TermMatch TermQuery<TNumeric>(in FieldMetadata field, TNumeric term, CompactTree termsTree = null)
    {
        var containerId = GetContainerIdOfNumericalTerm(field, out var numericalField, term);

        return containerId == -1 
            ? TermMatch.CreateEmpty() 
            : TermQuery(numericalField, containerId, 1);
    }
    
    public CompactTree GetTermsFor(Slice name) => _fieldsTree != null && _fieldsTree.TryGetCompactTreeFor(name, out var terms) ? terms : null;

    public Lookup<Int64LookupKey> GetLongTermsFor(Slice name) => _fieldsTree != null && _fieldsTree.TryGetLookupFor<Int64LookupKey>(name, out var longTerms) ? longTerms : null;

    public Lookup<DoubleLookupKey> GetDoubleTermsFor(Slice name) => _fieldsTree != null && _fieldsTree.TryGetLookupFor<DoubleLookupKey>(name, out var doubleTerms) ? doubleTerms : null;

    public TermMatch TermQuery(in FieldMetadata field, string term, CompactTree termsTree = null)
    {
        return TryGetTermId(field, term, out var termId, out var termRatioToWholeCollection, termsTree)
            ? TermQuery(field, termId, termRatioToWholeCollection)
            : TermMatch.CreateEmpty();
    }

    //Should be already analyzed...
    public TermMatch TermQuery(in FieldMetadata field, Slice term, CompactTree termsTree = null)
    {
        return TryGetTermId(field, term, out var termId, out var termRatioToWholeCollection, termsTree)
            ? TermQuery(field, termId, termRatioToWholeCollection)
            : TermMatch.CreateEmpty();
    }

    public TermMatch TermQuery(in FieldMetadata field, CompactKey term, CompactTree tree)
    {
        if (TryGetTermId(field, term, tree, out var termId, out var termRatioToWholeCollection) == false)
            return TermMatch.CreateEmpty();

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
        var terms = termsTree ?? GetTermsFor(field.FieldName);
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
        var terms = termsTree ?? GetTermsFor(field.FieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty.
            return false;
        }

        if (term.Size == 0)
        {
            // An empty term matches nothing (mirrors the analyzed-string overload); passing a null CompactKey
            // into CompactTree.TryGetValue would throw.
            return false;
        }

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
            totalSum = totalSumReader.Read<long>();

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
            matches = TermMatch.YieldSet(this, Allocator, postingList, termRatioToWholeCollection, isBoosting);
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

    /// <summary>
    /// Returns the raw posting list ID (with TermIdMask encoding) for a string term,
    /// or -1 if the term does not exist in the index.
    /// </summary>
    public long GetTermPostingListId(in FieldMetadata field, string term)
    {
        var terms = GetTermsFor(field.FieldName);
        if (terms == null)
            return -1;

        if (term is null || ReferenceEquals(term, Constants.ProjectionNullValue))
            return TryGetPostingListForNull(field, out var plId) ? plId : -1;

        // A term the analyzer splits into != 1 token has no single posting list id.
        if (TryAnalyzeSingleToken(field, term, out var termSlice) == false)
            return -1;

        if (termSlice.Size == 0)
            return -1;

        using var termKeyScope = new CompactKeyCacheScope(_fieldsTree.Llt);
        var termKey = termKeyScope.Key;
        termKey.Set(termSlice.AsReadOnlySpan());
        return terms.TryGetValue(termKey, out var value) ? value : -1;
    }

    /// <summary>
    /// Returns the raw posting list ID (with TermIdMask encoding) for a numeric term,
    /// or -1 if the term does not exist in the index. Mirrors the resolution path
    /// taken by <see cref="TermQuery{TNumeric}"/>.
    /// </summary>
    public long GetTermPostingListId<TNumeric>(in FieldMetadata field, TNumeric term)
    {
        return GetContainerIdOfNumericalTerm(field, out _, term);
    }

    /// <summary>
    /// Returns the raw posting list ID (with TermIdMask encoding) for a Slice term,
    /// or -1 if the term does not exist in the index.
    /// </summary>
    public long GetTermPostingListId(in FieldMetadata field, Slice term)
    {
        var terms = GetTermsFor(field.FieldName);
        if (terms == null)
            return -1;

        if (term.Size == 0)
            return -1;

        using var termKeyScope = new CompactKeyCacheScope(_fieldsTree.Llt);
        var termKey = termKeyScope.Key;
        termKey.Set(term.AsReadOnlySpan());
        return terms.TryGetValue(termKey, out var value) ? value : -1;
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
        var terms = GetTermsFor(binding.FieldName);
        if (terms == null && term != null)
            return 0;
        
        if (term is null || ReferenceEquals(term, Constants.ProjectionNullValue))
        {
            var termMatch =  TryGetPostingListForNull(binding, out var postingListId) 
                ? TermQuery(binding, postingListId, 1D) 
                : TermMatch.CreateEmpty();
            return termMatch.Count;
        }
        
        // A multi-token input (e.g. MoreLikeThis passing un-tokenized text) matches no single indexed term.
        if (TryAnalyzeSingleToken(binding, term, out var termSlice) == false)
            return 0;

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
