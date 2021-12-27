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
using System.Data;
using System.IO;
using Voron.Data.CompactTrees;
using Sparrow;
using static Corax.Queries.SortingMatch;
using System.Runtime.Intrinsics.X86;
using Sparrow.Server;
using Corax.Pipeline;
using NotContainsTermProvider = Corax.Queries.NotContainsTermProvider;

namespace Corax
{
    public sealed unsafe class IndexSearcher : IDisposable
    {        
        private const int NonAnalyzer = -1;

        private readonly Transaction _transaction;
        private readonly IndexFieldsMapping _fieldMapping;
        
        private Page _lastPage = default;

        /// <summary>
        /// When true no SIMD instruction will be used. Useful for checking that optimized algorithms behave in the same
        /// way than reference algorithms. 
        /// </summary>
        public bool ForceNonAccelerated { get; set; }

        public const int TakeAll = -1;

        public bool IsAccelerated => Avx2.IsSupported && !ForceNonAccelerated;

        public long NumberOfEntries => _transaction.LowLevelTransaction.RootObjects.ReadInt64(IndexWriter.NumberOfEntriesSlice) ?? 0;

        internal ByteStringContext Allocator => _transaction.Allocator;


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

        public string GetIdentityFor(long id)
        {
            var data = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref _lastPage, id).ToSpan();
            int size = ZigZagEncoding.Decode<int>(data, out var len);
            return Encoding.UTF8.GetString(data.Slice(len, size));
        }

        public AllEntriesMatch AllEntries()
        {
            return new AllEntriesMatch(_transaction);
        }

        // foreach term in 2010 .. 2020
        //     yield return TermMatch(field, term)// <-- one term , not sorted

        // userid = UID and date between 2010 and 2020 <-- 100 million terms here 
        // foo = bar and published = true

        // foo = bar
        public TermMatch TermQuery(string field, string term, int fieldId = NonAnalyzer)
        {
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            if (fields == null)
                return TermMatch.CreateEmpty();
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return TermMatch.CreateEmpty();

            return TermQuery(terms, term, fieldId);
        }

        private TermMatch TermQuery(CompactTree tree, string term, int fieldId = NonAnalyzer)
        {
            if (tree.TryGetValue(EncodeTerm(term, fieldId), out var value) == false)
                return TermMatch.CreateEmpty();

            TermMatch matches;
            if ((value & (long)TermIdMask.Set) != 0)
            {
                var setId = value & ~0b11;
                var setStateSpan = Container.Get(_transaction.LowLevelTransaction, setId).ToSpan();
                ref readonly var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
                var set = new Set(_transaction.LowLevelTransaction, Slices.Empty, setState);
                matches = TermMatch.YieldSet(set, IsAccelerated);
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
#if DEBUG
            matches.Term = term;
#endif
            return matches;
        }

        public MultiTermMatch InQuery(string field, List<string> inTerms)
        {
            // TODO: The IEnumerable<string> will die eventually, this is for prototyping only. 
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return MultiTermMatch.CreateEmpty(_transaction.Allocator);

            if (inTerms.Count is > 1 and <= 16)
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

            return MultiTermMatch.Create(new MultiTermMatch<InTermProvider>(_transaction.Allocator, new InTermProvider(this, field, inTerms)));
        }

        public MultiTermMatch StartWithQuery(string field, string startWith, bool isNegated = false, int fieldId = NonAnalyzer)
        {
            return PrefixSuffixQuery<StartWithTermProvider>(field, startWith, isNegated, fieldId);
        }

        public MultiTermMatch ExistsQuery(string field)
        {
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return MultiTermMatch.CreateEmpty(_transaction.Allocator);

            return MultiTermMatch.Create(new MultiTermMatch<ExistsTermProvider>(_transaction.Allocator,
                new ExistsTermProvider(this, _transaction.Allocator, terms, field)));
        }

        public MultiTermMatch EndsWithQuery(string field, string endsWith, bool isNegated = false, int fieldId = NonAnalyzer)
        {
            return PrefixSuffixQuery<EndsWithTermProvider>(field, endsWith, isNegated, fieldId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private MultiTermMatch PrefixSuffixQuery<TTermProvider>(string field, string term, bool isNegated = false, int fieldId = NonAnalyzer)
            where TTermProvider : ITermProvider
        {
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return MultiTermMatch.CreateEmpty(_transaction.Allocator);

            if (typeof(TTermProvider) == typeof(StartWithTermProvider))
            {
                return isNegated
                    ? MultiTermMatch.Create(new MultiTermMatch<NotStartWithTermProvider>(_transaction.Allocator,
                        new NotStartWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, term)))
                    : MultiTermMatch.Create(new MultiTermMatch<StartWithTermProvider>(_transaction.Allocator,
                        new StartWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, term)));
            }

            if (typeof(TTermProvider) == typeof(EndsWithTermProvider))
            {
                return isNegated
                    ? MultiTermMatch.Create(new MultiTermMatch<NotEndsWithTermProvider>(_transaction.Allocator,
                        new NotEndsWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, term)))
                    : MultiTermMatch.Create(new MultiTermMatch<EndsWithTermProvider>(_transaction.Allocator,
                        new EndsWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, term)));
            }

            return MultiTermMatch.CreateEmpty(_transaction.Allocator);
        }

        public MultiTermMatch ContainsQuery(string field, string containsTerm, bool isNegated = false, int fieldId = NonAnalyzer)
        {
            // TODO: The IEnumerable<string> will die eventually, this is for prototyping only. 
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return MultiTermMatch.CreateEmpty(_transaction.Allocator);


            return isNegated
                ? MultiTermMatch.Create(new MultiTermMatch<NotContainsTermProvider>(_transaction.Allocator,
                    new NotContainsTermProvider(this, _transaction.Allocator, terms, field, containsTerm, fieldId)))
                : MultiTermMatch.Create(new MultiTermMatch<ContainsTermProvider>(_transaction.Allocator,
                    new ContainsTermProvider(this, _transaction.Allocator, terms, field, containsTerm, fieldId)));
        }

        public SortingMatch OrderByAscending<TInner>(in TInner set, int fieldId, MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence,
            int take = TakeAll)
            where TInner : IQueryMatch
        {
            return OrderBy<TInner, AscendingMatchComparer>(in set, fieldId, entryFieldType, take);
        }

        public SortingMatch OrderByDescending<TInner>(in TInner set, int fieldId, MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence,
            int take = TakeAll)
            where TInner : IQueryMatch
        {
            return OrderBy<TInner, DescendingMatchComparer>(in set, fieldId, entryFieldType, take);
        }

        public SortingMatch OrderBy<TInner, TComparer>(in TInner set, int fieldId, MatchCompareFieldType entryFieldType = MatchCompareFieldType.Sequence,
            int take = TakeAll)
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

        public SortingMatch OrderByScore<TInner>(in TInner set, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return Create(new SortingMatch<TInner, BoostingComparer>(this, set, default(BoostingComparer), take));
        }

        public SortingMatch OrderBy<TInner, TComparer>(in TInner set, in TComparer comparer, int take = TakeAll)
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
            int take = TakeAll)
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

        public UnaryMatch GreaterThan<TInner>(in TInner set, int fieldId, Slice value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, Slice>.YieldGreaterThan(set, this, fieldId, value, take));
        }

        public UnaryMatch GreaterThan<TInner>(in TInner set, int fieldId, long value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, long>.YieldGreaterThan(set, this, fieldId, value, take));
        }

        public UnaryMatch GreaterThan<TInner>(in TInner set, int fieldId, double value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, double>.YieldGreaterThan(set, this, fieldId, value, take));
        }

        public UnaryMatch GreaterThanOrEqual<TInner>(in TInner set, int fieldId, Slice value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, Slice>.YieldGreaterThanOrEqualMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch GreaterThanOrEqual<TInner>(in TInner set, int fieldId, long value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, long>.YieldGreaterThanOrEqualMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch GreaterThanOrEqual<TInner>(in TInner set, int fieldId, double value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, double>.YieldGreaterThanOrEqualMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch LessThan<TInner>(in TInner set, int fieldId, Slice value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, Slice>.YieldLessThan(set, this, fieldId, value, take));
        }

        public UnaryMatch LessThan<TInner>(in TInner set, int fieldId, long value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, long>.YieldLessThan(set, this, fieldId, value, take));
        }

        public UnaryMatch LessThan<TInner>(in TInner set, int fieldId, double value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, double>.YieldLessThan(set, this, fieldId, value, take));
        }

        public UnaryMatch LessThanOrEqual<TInner>(in TInner set, int fieldId, Slice value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, Slice>.YieldLessThanOrEqualMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch LessThanOrEqual<TInner>(in TInner set, int fieldId, long value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, long>.YieldLessThanOrEqualMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch LessThanOrEqual<TInner>(in TInner set, int fieldId, double value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, double>.YieldLessThanOrEqualMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch Equals<TInner>(in TInner set, int fieldId, Slice value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, Slice>.YieldEqualsMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch Equals<TInner>(in TInner set, int fieldId, long value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, long>.YieldEqualsMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch Equals<TInner>(in TInner set, int fieldId, double value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, double>.YieldEqualsMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch NotEquals<TInner>(in TInner set, int fieldId, Slice value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, Slice>.YieldNotEqualsMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch NotEquals<TInner>(in TInner set, int fieldId, long value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, long>.YieldNotEqualsMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch NotEquals<TInner>(in TInner set, int fieldId, double value, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, double>.YieldNotEqualsMatch(set, this, fieldId, value, take));
        }

        public UnaryMatch Between<TInner>(in TInner set, int fieldId, Slice value1, Slice value2, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, Slice>.YieldBetweenMatch(set, this, fieldId, value1, value2, take));
        }

        public UnaryMatch Between<TInner>(in TInner set, int fieldId, long value1, long value2, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, long>.YieldBetweenMatch(set, this, fieldId, value1, value2, take));
        }

        public UnaryMatch Between<TInner>(in TInner set, int fieldId, double value1, double value2, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, double>.YieldBetweenMatch(set, this, fieldId, value1, value2, take));
        }

        public UnaryMatch NotBetween<TInner>(in TInner set, int fieldId, Slice value1, Slice value2, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, Slice>.YieldNotBetweenMatch(set, this, fieldId, value1, value2, take));
        }

        public UnaryMatch NotBetween<TInner>(in TInner set, int fieldId, long value1, long value2, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, long>.YieldNotBetweenMatch(set, this, fieldId, value1, value2, take));
        }

        public UnaryMatch NotBetween<TInner>(in TInner set, int fieldId, double value1, double value2, int take = TakeAll)
            where TInner : IQueryMatch
        {
            return UnaryMatch.Create(UnaryMatch<TInner, double>.YieldNotBetweenMatch(set, this, fieldId, value1, value2, take));
        }

        //TODO PERF Search
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlySpan<byte> EncodeTerm(string term, int fieldId)
        {
            var encodedTerm = Encoding.UTF8.GetBytes(term).AsSpan();
            return fieldId == NonAnalyzer ? encodedTerm : KeywordEncodeTerm(encodedTerm, fieldId);
        }

        public BoostingMatch Boost<TInner>(TInner match, float constant)
            where TInner : IQueryMatch
        {
            return BoostingMatch.WithConstant(this, match, constant);
        }

        public BoostingMatch Boost<TInner, TScoreFunction>(TInner match, TScoreFunction scoreFunction)
            where TInner : IQueryMatch
            where TScoreFunction : IQueryScoreFunction
        {
            if (typeof(TScoreFunction) == typeof(TermFrequencyScoreFunction))
            {
                return BoostingMatch.WithTermFrequency(this, match, (TermFrequencyScoreFunction)(object)scoreFunction);
            }
            else if (typeof(TScoreFunction) == typeof(ConstantScoreFunction))
            {
                return BoostingMatch.WithConstant(this, match, (ConstantScoreFunction)(object)scoreFunction);
            }
            else
            {
                throw new NotSupportedException($"Boosting with the score function {nameof(TScoreFunction)} is not supported.");
            }
        }

        public MultiTermMatch InQuery<TScoreFunction>(string field, List<string> inTerms, TScoreFunction scoreFunction)
            where TScoreFunction : IQueryScoreFunction
        {
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return MultiTermMatch.CreateEmpty(_transaction.Allocator);

            if (inTerms.Count is > 1 and <= 16)
            {
                var stack = new BinaryMatch[inTerms.Count / 2];
                for (int i = 0; i < inTerms.Count / 2; i++)
                {
                    var term1 = Boost(TermQuery(terms, inTerms[i * 2]), scoreFunction);
                    var term2 = Boost(TermQuery(terms, inTerms[i * 2 + 1]), scoreFunction);
                    stack[i] = Or(term1, term2);
                }

                if (inTerms.Count % 2 == 1)
                {
                    // We need even values to make the last work. 
                    var term = Boost(TermQuery(terms, inTerms[^1]), scoreFunction);
                    stack[^1] = Or(stack[^1], term);
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

            return MultiTermMatch.Create(
                MultiTermBoostingMatch<InTermProvider>.Create(
                    this, new InTermProvider(this, field, inTerms), scoreFunction));
        }

        public MultiTermMatch StartWithQuery<TScoreFunction>(string field, string startWith, TScoreFunction scoreFunction, bool isNegated = false,
            int fieldId = NonAnalyzer)
            where TScoreFunction : IQueryScoreFunction
        {
            return PrefixSuffixQuery<TScoreFunction, StartWithTermProvider>(field, startWith, scoreFunction, isNegated, fieldId);
        }

        public MultiTermMatch EndsWithQuery<TScoreFunction>(string field, string endsWith, TScoreFunction scoreFunction, bool isNegated = false,
            int fieldId = NonAnalyzer)
            where TScoreFunction : IQueryScoreFunction
        {
            return PrefixSuffixQuery<TScoreFunction, EndsWithTermProvider>(field, endsWith, scoreFunction, isNegated, fieldId);
        }

        private MultiTermMatch PrefixSuffixQuery<TScoreFunction, TTermProvider>(string field, string term, TScoreFunction scoreFunction, bool isNegated = false,
            int fieldId = NonAnalyzer)
            where TScoreFunction : IQueryScoreFunction
            where TTermProvider : ITermProvider
        {
            // TODO: The IEnumerable<string> will die eventually, this is for prototyping only. 
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return MultiTermMatch.CreateEmpty(_transaction.Allocator);

            if (typeof(TTermProvider) == typeof(StartWithTermProvider))
            {
                return isNegated
                    ? MultiTermMatch.Create(
                        MultiTermBoostingMatch<NotStartWithTermProvider>.Create(
                            this, new NotStartWithTermProvider(this, _transaction.Allocator, terms, field, NonAnalyzer, term), scoreFunction))
                    : MultiTermMatch.Create(
                        MultiTermBoostingMatch<StartWithTermProvider>.Create(
                            this, new StartWithTermProvider(this, _transaction.Allocator, terms, field, NonAnalyzer, term), scoreFunction));
            }

            if (typeof(TTermProvider) == typeof(EndsWithTermProvider))
            {
                return isNegated
                    ? MultiTermMatch.Create(
                        MultiTermBoostingMatch<NotEndsWithTermProvider>.Create(
                            this, new NotEndsWithTermProvider(this, _transaction.Allocator, terms, field, NonAnalyzer, term), scoreFunction))
                    : MultiTermMatch.Create(
                        MultiTermBoostingMatch<EndsWithTermProvider>.Create(
                            this, new EndsWithTermProvider(this, _transaction.Allocator, terms, field, NonAnalyzer, term), scoreFunction));
            }

            return MultiTermMatch.CreateEmpty(_transaction.Allocator);
        }

        private MultiTermMatch ContainsQuery<TScoreFunction>(string field, string containsTerm, TScoreFunction scoreFunction, bool isNegated = false,
            int fieldId = NonAnalyzer)
            where TScoreFunction : IQueryScoreFunction
        {
            // TODO: The IEnumerable<string> will die eventually, this is for prototyping only. 
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null)
                return MultiTermMatch.CreateEmpty(_transaction.Allocator);

            if (isNegated == false)
                return MultiTermMatch.Create(
                    MultiTermBoostingMatch<ContainsTermProvider>.Create(
                        this, new ContainsTermProvider(this, _transaction.Allocator, terms, field, containsTerm, fieldId), scoreFunction));

            return MultiTermMatch.Create(
                MultiTermBoostingMatch<NotContainsTermProvider>.Create(
                    this, new NotContainsTermProvider(this, _transaction.Allocator, terms, field, containsTerm, fieldId), scoreFunction));
        }

        public IQueryMatch SearchQuery(string field, string searchTerm, SearchOperator @operator, bool isNegated, int analyzerId)
        {
            return SearchQuery<NullScoreFunction>(field, searchTerm, default, @operator, analyzerId, isNegated);
        }

        public IQueryMatch SearchQuery<TScoreFunction>(string field, string searchTerm, TScoreFunction scoreFunction, SearchOperator @operator, int analyzerId,
            bool isNegated = false)
            where TScoreFunction : IQueryScoreFunction
        {
            const byte wildcard = (byte)'*';
            ReadOnlySpan<byte> term = Encoding.UTF8.GetBytes(searchTerm).AsSpan();
            if (isNegated)
            {
                @operator = @operator switch
                {
                    SearchOperator.Or => SearchOperator.And,
                    SearchOperator.And => SearchOperator.Or,
                    _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
                };
            }

            if (_analyzers.TryGetValue(analyzerId, out var searchAnalyzer) == false)
            {
                throw new InvalidDataException($"{nameof(SearchQuery)} requires analyzer.");
            }

            var wildcardAnalyzer = Analyzer.Create<WhitespaceTokenizer, ExactTransformer>();

            searchAnalyzer.GetOutputBuffersSize(searchTerm.Length, out var outputSize, out var tokenSize);
            wildcardAnalyzer.GetOutputBuffersSize(searchTerm.Length, out var wildcardSize, out var wildcardTokenSize);

            var tokenStructSize = Unsafe.SizeOf<Token>();
            var buffer = QueryContext.MatchesPool.Rent(outputSize + wildcardSize + tokenStructSize * (tokenSize + wildcardTokenSize));
            Span<byte> encodeBufferOriginal = buffer.AsSpan().Slice(0, outputSize);
            Span<Token> tokensBufferOriginal = MemoryMarshal.Cast<byte, Token>(buffer.AsSpan().Slice(outputSize, tokenSize * tokenStructSize));

            var wildcardAnalyzerBuffer = buffer.AsSpan().Slice(outputSize + tokenSize * tokenStructSize, wildcardSize);
            var wildcardTokenizerBuffer = MemoryMarshal.Cast<byte, Token>(buffer.AsSpan().Slice(outputSize + tokenSize * tokenStructSize + wildcardSize));

            var encodedWildcard = wildcardAnalyzerBuffer;
            var tokensWildcard = wildcardTokenizerBuffer;


            wildcardAnalyzer.Execute(term, ref encodedWildcard, ref tokensWildcard);
            IQueryMatch match = null;
            foreach (var tokenWildcard in tokensWildcard)
            {
                var encoded = encodeBufferOriginal;
                var tokens = tokensBufferOriginal;
                var originalWord = term.Slice(tokenWildcard.Offset, (int)tokenWildcard.Length);
                searchAnalyzer.Execute(term.Slice(tokenWildcard.Offset, (int)tokenWildcard.Length), ref encoded, ref tokens);
                if (tokens.Length == 0)
                    continue;


                SearchMatchOptions mode = SearchMatchOptions.Exact;
                if (originalWord[0] is wildcard)
                    mode |= SearchMatchOptions.EndsWith;
                if (originalWord[^1] is wildcard)
                    mode |= SearchMatchOptions.StartsWith;
                if (mode.HasFlag(SearchMatchOptions.StartsWith | SearchMatchOptions.EndsWith))
                    mode = SearchMatchOptions.Contains;

                Slice.From(_transaction.Allocator, encoded.Slice(tokens[0].Offset, (int)tokens[0].Length), ByteStringType.Immutable, out var encodedString);
                if (typeof(TScoreFunction) == typeof(NullScoreFunction))
                    BuildExpressionWithoutScoring(mode, encodedString);
                else
                    BuildExpressionWithScoring(mode, encodedString);
            }

            QueryContext.MatchesPool.Return(buffer);
          
            return match;
            
            void BuildExpressionWithoutScoring(SearchMatchOptions mode, Slice encodedString)
            {
                   switch (mode)
                {
                    case SearchMatchOptions.Exact:
                        IQueryMatch exactMatch = isNegated
                            ? NotEquals(AllEntries(), analyzerId, encodedString)
                            : TermQuery(field, encodedString.ToString());

                        if (match is null)
                        {
                            match = exactMatch;
                            return;
                        }

                        match = @operator is SearchOperator.Or
                            ? Or(match, exactMatch)
                            : And(match, exactMatch);
                        break;
                    case SearchMatchOptions.StartsWith:
                        if (match is null)
                        {
                            match = StartWithQuery(field, encodedString.ToString(), isNegated);
                            return;
                        }

                        match = @operator is SearchOperator.Or
                            ? Or(match, StartWithQuery(field, encodedString.ToString(), isNegated))
                            : And(match, StartWithQuery(field, encodedString.ToString(), isNegated));
                        break;
                    case SearchMatchOptions.EndsWith:
                        if (match is null)
                        {
                            match = EndsWithQuery(field, encodedString.ToString(), isNegated);
                            return;
                        }

                        match = @operator is SearchOperator.Or
                            ? Or(match, EndsWithQuery(field, encodedString.ToString(), isNegated))
                            : And(match, EndsWithQuery(field, encodedString.ToString(), isNegated));
                        break;
                    case SearchMatchOptions.Contains:
                        if (match is null)
                        {
                            match = ContainsQuery(field, encodedString.ToString(), isNegated);
                            return;
                        }

                        match = @operator is SearchOperator.Or
                            ? Or(match, ContainsQuery(field, encodedString.ToString(), isNegated))
                            : And(match, ContainsQuery(field, encodedString.ToString(), isNegated));
                        break;
                    default:
                        throw new InvalidExpressionException("Unknown flag inside Search match.");
                }
            }
            void BuildExpressionWithScoring(SearchMatchOptions mode, Slice encodedString)
            {
                switch (mode)
                {
                    case SearchMatchOptions.Exact:
                        IQueryMatch exactMatch = isNegated
                            ? NotEquals(AllEntries(), analyzerId, encodedString)
                            : TermQuery(field, encodedString.ToString());

                        if (match is null)
                        {
                            match = exactMatch;
                            return;
                        }

                        match = @operator is SearchOperator.Or
                            ? Or(match, exactMatch)
                            : And(match, exactMatch);
                        break;
                    case SearchMatchOptions.StartsWith:
                        if (match is null)
                        {
                            match = StartWithQuery(field, encodedString.ToString(), scoreFunction, isNegated);
                            return;
                        }

                        match = @operator is SearchOperator.Or
                            ? Or(match, StartWithQuery(field, encodedString.ToString(), scoreFunction, isNegated))
                            : And(match, StartWithQuery(field, encodedString.ToString(), scoreFunction, isNegated));
                        break;
                    case SearchMatchOptions.EndsWith:
                        if (match is null)
                        {
                            match = EndsWithQuery(field, encodedString.ToString(), scoreFunction, isNegated);
                            return;
                        }

                        match = @operator is SearchOperator.Or
                            ? Or(match, EndsWithQuery(field, encodedString.ToString(), scoreFunction, isNegated))
                            : And(match, EndsWithQuery(field, encodedString.ToString(), scoreFunction, isNegated));
                        break;
                    case SearchMatchOptions.Contains:
                        if (match is null)
                        {
                            match = ContainsQuery(field, encodedString.ToString(), scoreFunction, isNegated);
                            return;
                        }

                        match = @operator is SearchOperator.Or
                            ? Or(match, ContainsQuery(field, encodedString.ToString(), scoreFunction, isNegated))
                            : And(match, ContainsQuery(field, encodedString.ToString(), scoreFunction, isNegated));
                        break;
                    default:
                        throw new InvalidExpressionException("Unknown flag inside Search match.");
                }
            }
        }

        [Flags]
        private enum SearchMatchOptions
        {
            Exact = 0,
            StartsWith = 1,
            EndsWith = 2,
            Contains = 4
        }


        public enum SearchOperator
        {
            Or,
            And
        }

        public BoostingMatch Boost<TInner>(TInner match, IQueryScoreFunction scoreFunction)
            where TInner : IQueryMatch
        {
            if (scoreFunction.GetType() == typeof(TermFrequencyScoreFunction))
            {
                return BoostingMatch.WithTermFrequency(this, match, (TermFrequencyScoreFunction)(object)scoreFunction);
            }
            else if (scoreFunction.GetType() == typeof(ConstantScoreFunction))
            {
                return BoostingMatch.WithConstant(this, match, (ConstantScoreFunction)(object)scoreFunction);
            }
            else
            {
                throw new NotSupportedException($"Boosting with the score function {scoreFunction.GetType().Name} is not supported.");
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlySpan<byte> EncodeTerm(Slice term, int fieldId)
        {
            return fieldId == NonAnalyzer ? term.AsReadOnlySpan() : KeywordEncodeTerm(term.AsSpan(), fieldId);
        }

        //todo maciej: notice this is very inefficient. We need to improve it in future. 
        // Only for KeywordTokenizer
        [SkipLocalsInit]
        private unsafe ReadOnlySpan<byte> KeywordEncodeTerm(Span<byte> originalTerm, int fieldId)
        {
            if (_fieldMapping.Count == 0)
                return originalTerm;

            var binding = _fieldMapping.GetByFieldId(fieldId);

            var analyzer = binding.Analyzer;

            if (analyzer is null)
                return originalTerm;
            
            analyzer.GetOutputBuffersSize(originalTerm.Length, out int outputSize, out int tokenSize);

            Span<byte> encoded = new byte[outputSize];
            Token* tokensPtr = stackalloc Token[tokenSize];
            var tokens = new Span<Token>(tokensPtr, tokenSize);
            analyzer.Execute(originalTerm, ref encoded, ref tokens);

            return encoded;
        }

        public void Dispose()
        {
            if (_ownsTransaction)
                _transaction?.Dispose();
        }
    }
}
