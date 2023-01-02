using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Mappings;
using Corax.Queries;
using Sparrow.Compression;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.PostingLists;

namespace Corax;

public partial class IndexSearcher
{
    /// <summary>
    ///  Test API, should not be used anywhere else
    /// </summary>
    public TermMatch TermQuery(string field, string term) => TermQuery(FieldMetadataBuilder(field), term);
    public TermMatch TermQuery(string field, Slice term) => TermQuery(FieldMetadataBuilder(field), term);
    public TermMatch TermQuery(Slice field, Slice term) => TermQuery(FieldMetadata.Build(field, default, default, default), term);

    
    public TermMatch TermQuery(FieldMetadata field, string term, CompactTree termsTree = null)
    {
        var terms = termsTree ?? _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return TermMatch.CreateEmpty(this, Allocator);
        }

        var termSlice = term switch
        {
            Constants.NullValue => Constants.NullValueSlice,
            Constants.EmptyString => Constants.EmptyStringSlice,
            _ => EncodeAndApplyAnalyzer(field, term)
        };
        
        return TermQuery(field, terms, termSlice.AsReadOnlySpan());
    }
    
    //Should be already analyzed...
    public TermMatch TermQuery(FieldMetadata field, Slice term, CompactTree termsTree = null)
    {
        var terms = termsTree ?? _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return TermMatch.CreateEmpty(this, Allocator);
        }
        
        return TermQuery(field, terms, term);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal TermMatch TermQuery(in FieldMetadata field, CompactTree tree, Slice term)
    {
        return TermQuery(field, tree, term.AsReadOnlySpan());
    }
    
    internal TermMatch TermQuery(in FieldMetadata field, CompactTree tree, ReadOnlySpan<byte> term)
    {
        if (tree.TryGetValue(term, out var value) == false)
            return TermMatch.CreateEmpty(this, Allocator);

        var matches = TermQuery(field, value);
        
#if DEBUG
        matches.Term = Encoding.UTF8.GetString(term);
#endif
        return matches;
    }

    internal TermMatch TermQuery(in FieldMetadata field, long containerId)
    {
        TermMatch matches;
        if ((containerId & (long)TermIdMask.Set) != 0)
        {
            var setId = containerId & Constants.StorageMask.ContainerType;
            var setStateSpan = Container.Get(_transaction.LowLevelTransaction, setId).ToSpan();

            ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
            var set = new PostingList(_transaction.LowLevelTransaction, Slices.Empty, setState);
            matches = field.CalculateScoring 
                ? TermMatch.YieldSetWithFreq(this, Allocator, set, IsAccelerated) 
                : TermMatch.YieldSetNoFreq(this, Allocator, set, IsAccelerated);
        }
        else if ((containerId & (long)TermIdMask.Small) != 0)
        {
            var smallSetId = containerId & Constants.StorageMask.ContainerType;
            var small = Container.Get(_transaction.LowLevelTransaction, smallSetId);
            matches = field.CalculateScoring ? 
                TermMatch.YieldSmallWithFreq(this, Allocator, small) : 
                TermMatch.YieldSmallNoFreq(this, Allocator, small);
        }
        else
        {
            matches = field.CalculateScoring 
                ? TermMatch.YieldOnceWithFreq(this, Allocator, containerId)
                : TermMatch.YieldOnceNoFreq(this, Allocator, containerId);
        }

        return matches;
    }

    public long TermAmount(FieldMetadata binding, string term)
    {
        var terms = _fieldsTree?.CompactTreeFor(binding.FieldName);
        if (terms == null)
            return 0;
        
        var termSlice = term switch
        {
            Constants.NullValue => Constants.NullValueSlice,
            Constants.EmptyString => Constants.EmptyStringSlice,
            _ => EncodeAndApplyAnalyzer(binding, term)
        };
        
        return TermAmount(terms, termSlice);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long TermAmount(CompactTree tree, Slice term)
    {
        return TermAmount(tree, term.AsReadOnlySpan());
    }
    
    internal long TermAmount(CompactTree tree, ReadOnlySpan<byte> term)
    {
        if (tree.TryGetValue(term, out var value) == false)
            return 0;

        if ((value & (long)TermIdMask.Set) != 0)
        {
            var setId = value & Constants.StorageMask.ContainerType;
            var setStateSpan = Container.Get(_transaction.LowLevelTransaction, setId).ToSpan();
            ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
            return setState.NumberOfEntries;
        }
        
        if ((value & (long)TermIdMask.Small) != 0)
        {
            var smallSetId = value & Constants.StorageMask.ContainerType;
            var small = Container.Get(_transaction.LowLevelTransaction, smallSetId);
            var itemsCount = ZigZagEncoding.Decode<int>(small.ToSpan(), out var len);

            return itemsCount;
        }

        return 1;
    }
}
