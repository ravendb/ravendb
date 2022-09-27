using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Queries;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Compression;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Sets;

namespace Corax;

public unsafe partial class IndexSearcher
{
    public TermMatch TermQuery(string field, string term, int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(field);
        
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return TermMatch.CreateEmpty(Allocator);
        }

        Slice termSlice;
        if (term == Constants.NullValue)
            termSlice = Constants.NullValueSlice;
        else if (term == Constants.EmptyString)
            termSlice = Constants.EmptyStringSlice;
        else
            termSlice = EncodeAndApplyAnalyzer(term, fieldId);

        return TermQuery(terms, termSlice, fieldId);
    }

    //This overload will die with current impl of InQuery
    internal TermMatch TermQuery(CompactTree tree, string term, int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        return TermQuery(tree, EncodeAndApplyAnalyzer(term, fieldId), fieldId);
    }

    internal TermMatch TermQuery(string field, Slice term)
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(field);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return TermMatch.CreateEmpty(Allocator);
        }

        return TermQuery(terms, term);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal TermMatch TermQuery(CompactTree tree, Slice term, int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        return TermQuery(tree, term.AsReadOnlySpan(), fieldId);
    }

    internal TermMatch TermQuery(CompactTree tree, ReadOnlySpan<byte> term, int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        if (tree.TryGetValue(term, out var value) == false)
            return TermMatch.CreateEmpty(Allocator);

        TermMatch matches;
        if ((value & (long)TermIdMask.Set) != 0)
        {
            var setId = value & Constants.StorageMask.ContainerType;
            var setStateSpan = Container.Get(_transaction.LowLevelTransaction, setId).ToSpan();
            ref readonly var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
            var set = new Set(_transaction.LowLevelTransaction, Slices.Empty, setState);
            matches = TermMatch.YieldSet(Allocator, set, IsAccelerated);
        }
        else if ((value & (long)TermIdMask.Small) != 0)
        {
            var smallSetId = value & Constants.StorageMask.ContainerType;
            var small = Container.Get(_transaction.LowLevelTransaction, smallSetId);
            matches = TermMatch.YieldSmall(Allocator, small);
        }
        else
        {
            matches = TermMatch.YieldOnce(Allocator, value);
        }
#if DEBUG
        matches.Term = Encoding.UTF8.GetString(term);
#endif
        return matches;
    }

    public long TermAmount(string field, string term, int fieldId)
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(field);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return 0;
        }

        if (term is null)
            return TermAmount(terms, Constants.NullValueSlice);
        if (term.Length == 0)
            return TermAmount(terms, Constants.EmptyStringSlice);

        var encodedSlice = EncodeAndApplyAnalyzer(term, fieldId);
        return TermAmount(terms, encodedSlice);
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
            ref readonly var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
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
