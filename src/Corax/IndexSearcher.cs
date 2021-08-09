using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Server.Compression;
using Voron;
using Voron.Impl;
using Voron.Data.Sets;
using Voron.Data.Containers;
using Corax.Queries;
using System.Collections.Generic;
using Voron.Data.CompactTrees;
using Sparrow;
using static Corax.Queries.SortingMatch;

namespace Corax
{
    public sealed unsafe class IndexSearcher : IDisposable
    {
        private readonly StorageEnvironment _environment;
        private readonly Transaction _transaction;

        private Page _lastPage = default;

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index searcher with opening semantics and also every new
        // searcher becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexSearcher(StorageEnvironment environment)
        {
            _environment = environment;
            _transaction = environment.ReadTransaction();
        }

        public UnmanagedSpan GetIndexEntryPointer(long id)
        {
            var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id);
            int size = ZigZagEncoding.Decode<int>(data.ToSpan(), out var len);
            return data.ToUnmanagedSpan().Slice(size + len);
        }

        public IndexEntryReader GetReaderFor(long id)
        {
            var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id).ToSpan();
            int size = ZigZagEncoding.Decode<int>(data, out var len);
            return new IndexEntryReader(data.Slice(size + len));
        }

        public string GetIdentityFor(long id)
        {
            var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id).ToSpan();
            int size = ZigZagEncoding.Decode<int>(data, out var len);
            return Encoding.UTF8.GetString(data.Slice(len, size));
        }

        // foreach term in 2010 .. 2020
        //     yield return TermMatch(field, term)// <-- one term , not sorted

        // userid = UID and date between 2010 and 2020 <-- 100 million terms here 
        // foo = bar and published = true

        // foo = bar
        public TermMatch TermQuery(string field, string term)
        {
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return TermMatch.CreateEmpty();

            return TermQuery(terms, term);
        }

        private TermMatch TermQuery(CompactTree tree, string term)
        {
            if (tree.TryGetValue(term, out var value) == false)
                return TermMatch.CreateEmpty();

            TermMatch matches;
            if ((value & (long)TermIdMask.Set) != 0)
            {
                var setId = value & ~0b11;
                var setStateSpan = Container.Get(_transaction.LowLevelTransaction, setId).ToSpan();
                ref readonly var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
                var set = new Set(_transaction.LowLevelTransaction, Slices.Empty, setState);
                matches = TermMatch.YieldSet(set);
            }
            else if ((value & (long)TermIdMask.Small) != 0)
            {
                var smallSetId = value & ~0b11;
                var small = Container.Get(_transaction.LowLevelTransaction, smallSetId);
                matches = TermMatch.YieldSmall(small);
            }
            else
            {
                matches = TermMatch.YieldOnce(value);
            }

            return matches;
        }

        public MultiTermMatch InQuery(string field, List<string> inTerms)
        {
            // TODO: The IEnumerable<string> will die eventually, this is for prototyping only. 
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return MultiTermMatch.CreateEmpty();

            if (inTerms.Count > 1 && inTerms.Count <= 16)
            {
                var stack = new BinaryMatch[inTerms.Count / 2];
                for (int i = 0; i < inTerms.Count / 2; i++)
                    stack[i] = Or(TermQuery(terms, inTerms[i * 2]), TermQuery(terms, inTerms[i * 2 + 1]));

                if (inTerms.Count % 2 == 1)
                {
                    // We need even values to make the last work. 
                    stack[^1] = Or(stack[^1], TermQuery(terms, inTerms[^1]));
                }

                int currentTerms = stack.Length;
                while (currentTerms > 1)
                {
                    int termsToProcess = currentTerms / 2;
                    int excessTerms = currentTerms % 2;

                    for (int i = 0; i < termsToProcess; i++)
                        stack[i] = Or(stack[i * 2], stack[i * 2 + 1]);

                    if (excessTerms != 0)
                        stack[termsToProcess - 1] = Or(stack[termsToProcess - 1], stack[currentTerms - 1]);

                    currentTerms = termsToProcess;
                }
                return MultiTermMatch.Create(stack[0]);
            }

            return MultiTermMatch.Create(new MultiTermMatch<InTermProvider>(new InTermProvider(this, field, 0, inTerms)));
        }

        public MultiTermMatch StartWithQuery(string field, string startWith)
        {
            // TODO: The IEnumerable<string> will die eventually, this is for prototyping only. 
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return MultiTermMatch.CreateEmpty();

            return MultiTermMatch.Create(new MultiTermMatch<StartWithTermProvider>(new StartWithTermProvider(this, _transaction.Allocator, terms, field, 0, startWith)));
        }

        public SortingMatch OrderByAscending<TInner>(in TInner set, int fieldId, MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence, int take = -1)
            where TInner : IQueryMatch
        {
            return OrderBy<TInner, AscendingMatchComparer>(in set, fieldId, MatchCompareFieldType.Sequence, take);
        }

        public SortingMatch OrderByDescending<TInner>(in TInner set, int fieldId, MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence, int take = -1)
            where TInner : IQueryMatch
        {
            return OrderBy<TInner, DescendingMatchComparer>(in set, fieldId, MatchCompareFieldType.Sequence, take);
        }

        public SortingMatch OrderBy<TInner, TComparer>(in TInner set, int fieldId, MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence, int take = -1)
            where TInner : IQueryMatch
            where TComparer : IMatchComparer
        {
            if (typeof(TComparer) == typeof(AscendingMatchComparer))
            {
                return Create(new SortingMatch<TInner, AscendingMatchComparer>(this, set, new AscendingMatchComparer(this, fieldId, entryFieldType), take));
            }
            else if (typeof(TComparer) == typeof(DescendingMatchComparer))
            {
                return Create(new SortingMatch<TInner, DescendingMatchComparer>(this, set, new DescendingMatchComparer(this, fieldId, entryFieldType), take));
            }
            else if (typeof(TComparer) == typeof(CustomMatchComparer))
            {
                throw new ArgumentException($"Custom comparers can only be created through the {nameof(OrderByCustomOrder)}");
            }

            throw new ArgumentException($"The comparer of type {typeof(TComparer).Name} is not supported. Isn't {nameof(OrderByCustomOrder)} the right call for it?");
        }

        public SortingMatch OrderBy<TInner, TComparer>(in TInner set, in TComparer comparer, int take = -1)
            where TInner : IQueryMatch
            where TComparer : struct, IMatchComparer
        {
            return Create(new SortingMatch<TInner, TComparer>(this, set, comparer, take));
        }

        public SortingMatch OrderByCustomOrder<TInner>(in TInner set, int fieldId, 
                delegate*<IndexSearcher, int, long, long, int> compareByIdFunc,
                delegate*<long, long, int> compareLongFunc,
                delegate*<double, double, int> compareDoubleFunc,
                delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, int> compareSequenceFunc,
                MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence, 
                int take = -1)
            where TInner : IQueryMatch     
        {
            // Federico: I don't even really know if we are going to find a use case for this. However, it was built for the purpose
            //           of showing that it is possible to build any custom group of functions. Why would we want to do this instead
            //           of just building a TComparer, I dont know. But for now the `CustomMatchComparer` can be built like this from
            //           static functions. 
            return Create(new SortingMatch<TInner, CustomMatchComparer>(
                                    this, set, 
                                    new CustomMatchComparer(
                                        this, fieldId,
                                        compareByIdFunc,
                                        compareLongFunc,
                                        compareDoubleFunc,
                                        compareSequenceFunc,
                                        entryFieldType
                                        ),
                                    take));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BinaryMatch And<TInner, TOuter>(in TInner set1, in TOuter set2)
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            // TODO: We need to create this code using a template or using typed delegates (which either way would need templating for boilerplate code generation)

            // We don't want an unknown size multiterm match to be subject to this optimization. When faced with one that is unknown just execute as
            // it was written in the query. If we don't have statistics the confidence will be Low, so the optimization wont happen.
            if (set1.Count < set2.Count && set1.Confidence >= QueryCountConfidence.Normal)
                return And(set2, set1);

            // If any of the generic types is not known to be a struct (calling from interface) the code executed will
            // do all the work to figure out what to emit. The cost is in instantiation not on execution.                         
            if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(TermMatch))
            {
                return BinaryMatch.Create(BinaryMatch<TermMatch, TermMatch>.YieldAnd((TermMatch)(object)set1, (TermMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(TermMatch))
            {
                return BinaryMatch.Create(BinaryMatch<BinaryMatch, TermMatch>.YieldAnd((BinaryMatch)(object)set1, (TermMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(BinaryMatch))
            {
                return BinaryMatch.Create(BinaryMatch<TermMatch, BinaryMatch>.YieldAnd((TermMatch)(object)set1, (BinaryMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(BinaryMatch))
            {
                return BinaryMatch.Create(BinaryMatch<BinaryMatch, BinaryMatch>.YieldAnd((BinaryMatch)(object)set1, (BinaryMatch)(object)set2));
            }

            return BinaryMatch.Create(BinaryMatch<TInner, TOuter>.YieldAnd(in set1, in set2));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BinaryMatch Or<TInner, TOuter>(in TInner set1, in TOuter set2)
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            // When faced with a MultiTermMatch and something else, lets first calculate the something else.
            if (set2.GetType() == typeof(MultiTermMatch) && set1.GetType() != typeof(MultiTermMatch))
                return Or(set2, set1);

            // If any of the generic types is not known to be a struct (calling from interface) the code executed will
            // do all the work to figure out what to emit. The cost is in instantiation not on execution. 
            if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(TermMatch))
            {
                return BinaryMatch.Create(BinaryMatch<TermMatch, TermMatch>.YieldOr((TermMatch)(object)set1, (TermMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(TermMatch))
            {
                return BinaryMatch.Create(BinaryMatch<BinaryMatch, TermMatch>.YieldOr((BinaryMatch)(object)set1, (TermMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(TermMatch) && set2.GetType() == typeof(BinaryMatch))
            {
                return BinaryMatch.Create(BinaryMatch<TermMatch, BinaryMatch>.YieldOr((TermMatch)(object)set1, (BinaryMatch)(object)set2));
            }
            else if (set1.GetType() == typeof(BinaryMatch) && set2.GetType() == typeof(BinaryMatch))
            {
                return BinaryMatch.Create(BinaryMatch<BinaryMatch, BinaryMatch>.YieldOr((BinaryMatch)(object)set1, (BinaryMatch)(object)set2));
            }

            return BinaryMatch.Create(BinaryMatch<TInner, TOuter>.YieldOr(in set1, in set2));
        }

        public void Dispose()
        {
            _transaction?.Dispose();
        }
    }
}
