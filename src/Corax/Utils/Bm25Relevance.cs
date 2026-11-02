using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Sparrow.Extensions;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.PostingLists;

namespace Corax.Utils;

//This is implementation of BM25F from this white-paper:
//https://www.researchgate.net/publication/45886647_Integrating_the_Probabilistic_Models_BM25BM25F_into_Lucene

public sealed unsafe class Bm25Relevance : IDisposable
{
    [ThreadStatic]
    internal static ArrayPool<TermRelevance> RelevancePool; 
    
    private readonly delegate*<Bm25Relevance, Span<long>, int, void> _processFunc;
    private readonly delegate*<Bm25Relevance, Span<long>, Span<float>, float, void> _scoreFunc;
    // Sorted variant of _scoreFunc: used when the caller guarantees `matches` is sorted ascending (the
    // 'order by score()' / SortInMemory path). It walks the term ids with a single forward cursor instead of a
    // per-match BinarySearch — see CalculateScoreSortedFromMemory.
    private readonly delegate*<Bm25Relevance, Span<long>, Span<float>, float, void> _scoreSortedFunc;

    /// <summary>
    /// The default score array value must be bigger than 0 because of support for document boost.
    /// This is necessary in case we use 'order by score()' without a WHERE clause, where the document boost is the only factor in the equation.
    /// So in order not to multiply by 0 let set it to be very small. BM25F is using sum, so this has no impact on the result. 
    /// </summary>
    public const float InitialScoreValue = 1 / 1_000_000f;

    /// <summary>
    /// Score contribution of a document matched by a constant-scoring clause (e.g. exists()),
    /// where existence carries no term-frequency signal.
    /// </summary>
    public const float ConstantScoreValue = 1f;

    private const int MaximumDocumentCapacity = MaxSizeOfStorage / (sizeof(long) + sizeof(short));
    private const int DynamicScoreBatchSize = 16 * 1024;
    private const int MaxSizeOfStorage = 1024 * 1024; //1MB;
    private const float BFactor = 0.25f;
    private const float K1 = 2f;

    /// <summary>
    /// This is L_c / Avl_c. This is ratio of current term length to whole collection under specific field.
    /// Since we're indexing in batches we cannot calculate average during indexing. So we store sum of length
    /// and then, during query calculate avg as total_sum / term_amount.
    ///
    /// Please notice that for numeric trees (like Double/Long) this is always one (since sizeof(T)/sizeof(T))
    /// </summary>
    private readonly float _termRatioToWholeCollection;
    private readonly long* _matchBuffer;
    private readonly short* _scoreBuffer;
    private readonly int _numberOfDocuments;
    private int _currentId;
    private readonly float _idf;
    
    //In a case when we don't want to persist matches in memory we want to have possibility to load them again from disk.
    private PostingList.Iterator _setIterator;

    private readonly IDisposable _memoryHolder;
    public readonly bool IsStored;
    private bool _isDisposed;
    private readonly int _bufferCapacity;
    private Span<long> Matches => new(_matchBuffer, _currentId);
    private Span<short> Scores => new(_scoreBuffer, _currentId);

    private Bm25Relevance(Querying.IndexSearcher indexSearcher, long termFrequency, ByteStringContext context, int numberOfDocuments, double termRatioToWholeCollection,
        delegate*<Bm25Relevance, Span<long>, Span<float>, float, void> dynamicalScoreFunc)
    {
        _termRatioToWholeCollection = (float)termRatioToWholeCollection;
        _numberOfDocuments = numberOfDocuments;
        IsStored = MaximumDocumentCapacity > numberOfDocuments;


        if (IsStored == false && dynamicalScoreFunc != null)
        {
            _scoreFunc = dynamicalScoreFunc;
            _scoreSortedFunc = &PostingListCalculateScoreSorted;
            _processFunc = &DecodeAndDiscard;
            _bufferCapacity = DynamicScoreBatchSize;
            _currentId = DynamicScoreBatchSize;
        }
        else
        {
            _processFunc = &DecodeAndSave;
            _scoreFunc = &CalculateScoreFromMemory;
            _scoreSortedFunc = &CalculateScoreSortedFromMemory;
            _bufferCapacity = numberOfDocuments;
            _currentId = 0;
        }

        _memoryHolder = context.Allocate(_bufferCapacity * (sizeof(long) + sizeof(short)), out var buffer);
        _matchBuffer = (long*)buffer.Ptr;
        _scoreBuffer = (short*)(buffer.Ptr + _bufferCapacity * sizeof(long));

        _idf = ComputeIdf(indexSearcher, termFrequency);
    }

    /// <summary>
    /// We add 1 to the IDF (Inverse Document Frequency) value to ensure that it is not equal to 0.
    /// This guarantees that the boost factor is not 'forgotten' in the calculation of the score. 
    /// </summary>
    internal static float ComputeIdf(Querying.IndexSearcher indexSearcher, long termFrequency)
    {
        var m = indexSearcher.NumberOfEntries - termFrequency + 0.5D;
        var d = termFrequency + 0.5D;

        return (float)Math.Log((m / d) + 1);
    }

    internal static float Denominator(float termRatioToWholeCollection) => (1 - BFactor) + BFactor * termRatioToWholeCollection;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static float Contribution(short frequency, float boostFactor, float denominator, float idf)
    {
        var weight = frequency * boostFactor / denominator;
        return idf * weight / (K1 + weight);
    }

    /// <summary>
    /// Scores a term that has a single document. It is kept inline instead of in a Bm25Relevance.
    /// </summary>
    internal static void ScoreSingle(Span<long> matches, Span<float> scores, float boostFactor, in TermRelevance term, float idf)
    {
        if (idf.AlmostEquals(0f))
            return;

        var idOfMatch = matches.BinarySearch(term.EntryId);
        if (idOfMatch >= 0)
            scores[idOfMatch] += Contribution(term.Frequency, boostFactor, term.Denominator, idf);
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (_isDisposed)
            ThrowAlreadyDisposed();

        _scoreFunc(this, matches, scores, boostFactor);
    }

    /// <summary>
    /// Same result as <see cref="Score"/>, but requires <paramref name="matches"/> to be sorted ascending.
    /// Exploits that the term ids are also sorted ascending to replace the per-match BinarySearch with a single
    /// forward-cursor (galloping) merge.
    /// </summary>
    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (_isDisposed)
            ThrowAlreadyDisposed();

        _scoreSortedFunc(this, matches, scores, boostFactor);
    }

    /// <summary>
    /// Legend (mapping code names to names from white-paper)
    /// _termRatioToWholeCollection - l_c / avg_c
    /// BFactor - B_c
    /// boostFactor - Boost_c
    /// frequencies - occurs
    /// </summary>
    /// <param name="matches">Ids of docs matched by query. Requirements: sorted</param>
    /// <param name="scores"></param>
    /// <param name="boostFactor">Scalar</param>
    private static void CalculateScoreFromMemory(Bm25Relevance bm25, Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (bm25._idf.AlmostEquals(0f))
            return;

        var innerItems = bm25.Matches;
        var frequencies = bm25.Scores;
        var denominator = Denominator(bm25._termRatioToWholeCollection);

        // Score() callers may pass matches in any order; the sorted merge lives in ScoreSortedRun.
        for (int idX = 0; idX < matches.Length; ++idX)
        {
            var idOfInner = innerItems.BinarySearch(matches[idX]);
            if (idOfInner < 0)
                continue;

            scores[idX] += Contribution(frequencies[idOfInner], boostFactor, denominator, bm25._idf);
        }
    }

    /// <summary>In-memory sorted scoring: a single forward-cursor merge of the (ascending) query matches against
    /// the (ascending) materialized term ids. Produces exactly the same hits as <see cref="CalculateScoreFromMemory"/>.</summary>
    private static void CalculateScoreSortedFromMemory(Bm25Relevance bm25, Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (bm25._idf.AlmostEquals(0f))
            return;

        int matchIdx = 0;
        ScoreSortedRun(bm25, matches, ref matchIdx, scores, boostFactor);
    }

    /// <summary>
    /// Galloping merge of two ascending sequences — the query <paramref name="matches"/> (from
    /// <paramref name="matchIdx"/> onward) and the term ids in <see cref="Matches"/> with their parallel
    /// frequencies in <see cref="Scores"/>. Scores every match present in the term set, advancing a single
    /// forward cursor over the term ids (exponential search) rather than re-searching from the start per match.
    /// <paramref name="matchIdx"/> is updated to the first match not yet decided, so the chunked posting-list
    /// path can resume across Fill batches without re-walking earlier matches. Equivalent in result to a
    /// per-match BinarySearch; only the lookup strategy differs.
    /// </summary>
    private static void ScoreSortedRun(Bm25Relevance bm25, Span<long> matches, ref int matchIdx, Span<float> scores, float boostFactor)
    {
        var innerItems = bm25.Matches;
        var frequencies = bm25.Scores;
        int innerLen = innerItems.Length;
        int matchesLen = matches.Length;

        int m = matchIdx;
        int n = 0;
        while (m < matchesLen && n < innerLen)
        {
            long target = matches[m];

            // Advance the term-id cursor to the first id >= target (galloping from the current position).
            n = Sorting.GallopLowerBound(innerItems, n, innerLen, target);
            if (n >= innerLen)
                break;

            long current = innerItems[n];
            if (current == target)
            {
                var weight = frequencies[n] * boostFactor / ((1 - BFactor) + BFactor * bm25._termRatioToWholeCollection);
                scores[m] += bm25._idf * weight / (K1 + weight);
                m++;
                n++;
            }
            else // current > target: target is absent from the term set; skip past every match below current.
            {
                m++;
                while (m < matchesLen && matches[m] < current)
                    m++;
            }
        }

        matchIdx = m;
    }

    /// <summary>Sorted counterpart of the dynamic (large posting list) path: streams posting-list chunks and
    /// merges each against the still-unprocessed tail of <paramref name="matches"/> with a forward cursor, so
    /// each match is visited once across the whole posting list (instead of being BinarySearch'd into every
    /// chunk). Stops early once every match has been decided.</summary>
    private static void PostingListCalculateScoreSorted(Bm25Relevance bm25, Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (bm25._idf.AlmostEquals(0f) || matches.IsEmpty)
            return;

        // TermMatch has already drained this posting list to build the candidate set, and both iterators navigate the
        // same PostingList tree cursor - without re-seating, this one would only ever see the leaf its own reader was
        // parked on. Seeking to the first candidate also skips every leaf below it.
        bm25._setIterator.Seek(EntryIdEncodings.PrepareIdForSeekInPostingList(matches[0]));

        int matchIdx = 0;
        bm25._currentId = bm25._bufferCapacity;
        while (matchIdx < matches.Length &&
               bm25._setIterator.Fill(bm25.Matches, out var read, pruneGreaterThanOptimization: EntryIdEncodings.PrepareIdForPruneInPostingList(matches[^1])) && read > 0)
        {
            bm25._currentId = read;
            // Same decode as the stored path (DecodeAndSave): the posting list yields entry id + quantized frequency
            // packed together, while `matches` holds plain entry ids. Without splitting them ScoreSortedRun never
            // finds a match and every frequency reads as zero.
            EntryIdEncodings.Decode(bm25.Matches, bm25.Scores);
            ScoreSortedRun(bm25, matches, ref matchIdx, scores, boostFactor);
            bm25._currentId = bm25._bufferCapacity;
        }
    }

    /// <summary>
    /// Index of the first item not smaller than the value, searched from start. Galloping pays off when the items are dense
    /// relative to the lookups, otherwise a binary search of the remaining items is cheaper.
    /// </summary>
    internal static int FindLowerBound(Span<long> items, int start, long value, bool gallop)
    {
        if (gallop == false)
        {
            var found = items[start..].BinarySearch(value);
            return start + (found >= 0 ? found : ~found);
        }

        if (start == items.Length || items[start] >= value)
            return start;

        var low = start;
        var step = 1;
        while (low + step < items.Length && items[low + step] < value)
        {
            low += step;
            step <<= 1;
        }

        var high = Math.Min(low + step, items.Length);
        low++;
        while (low < high)
        {
            var middle = (low + high) >>> 1;
            if (items[middle] < value)
                low = middle + 1;
            else
                high = middle;
        }

        return low;
    }

    /// <summary>
    /// Returns decoded spans of ids.
    /// </summary>
    public void Process(Span<long> matches, int count) => _processFunc(this, matches, count);

    private static void DecodeAndDiscard(Bm25Relevance bm25, Span<long> matches, int count)
    {
        EntryIdEncodings.DecodeAndDiscardFrequency(matches, count);
    }

    private static void DecodeAndSave(Bm25Relevance bm25, Span<long> matches, int count)
    {
        EntryIdEncodings.Decode(matches.Slice(0, count),
            new(bm25._scoreBuffer + bm25._currentId, bm25._numberOfDocuments - bm25._currentId));
        
        matches.Slice(0, count)
            .CopyTo(new Span<long>(bm25._matchBuffer + bm25._currentId, bm25._numberOfDocuments - bm25._currentId));
        
        bm25._currentId += count;
    }

    public long Add(long entry)
    {
        if (IsStored == false)
            return (long)EntryIdEncodings.Decode(entry).EntryId;

        var decoded = EntryIdEncodings.Decode(entry);
        *(_matchBuffer + _currentId) = (long)decoded.EntryId;
        *(_scoreBuffer + _currentId) = decoded.Frequency;
        _currentId += 1;

        return (long)*(_matchBuffer + _currentId - 1);
    }

    [DoesNotReturn]
    private void ThrowAlreadyDisposed()
    {
        throw new ObjectDisposedException($"{nameof(Bm25Relevance)} instance is already disposed.");
    }

    public void Remove()
    {
        _currentId -= 1;
    }

    public void Dispose()
    {
        if (_isDisposed)
            return;
        
        _isDisposed = true;
        _currentId = 0;
        _memoryHolder?.Dispose();
    }

    public static Bm25Relevance Once(Querying.IndexSearcher indexSearcher, long termFrequency, ByteStringContext context, int numberOfDocuments, double termRatioToWholeCollection)
    {
        return new(indexSearcher, termFrequency, context, numberOfDocuments, termRatioToWholeCollection, dynamicalScoreFunc: null);
    }

    public static Bm25Relevance Small(Querying.IndexSearcher indexSearcher, long termFrequency, ByteStringContext context, int numberOfDocuments,
        double termRatioToWholeCollection)
    {
        return new(indexSearcher, termFrequency, context, numberOfDocuments, termRatioToWholeCollection, dynamicalScoreFunc: null);
    }

    public static Bm25Relevance Set(Querying.IndexSearcher indexSearcher, long termFrequency, ByteStringContext context, int numberOfDocuments, double termRatioToWholeCollection,
        PostingList postingList)
    {
        static void PostingListCalculateScoreDynamically(Bm25Relevance bm25, Span<long> matches, Span<float> scores, float boostFactor)
        {
            if (matches.IsEmpty)
                return;

            // Same shared tree cursor as PostingListCalculateScoreSorted; `matches` is unsorted here, so start from the top.
            bm25._setIterator.Seek();

            bm25._currentId = bm25._bufferCapacity;
            while (bm25._setIterator.Fill(bm25.Matches, out var read, pruneGreaterThanOptimization: EntryIdEncodings.PrepareIdForPruneInPostingList(matches[^1])) && read > 0)
            {
                bm25._currentId = read;
                // The posting list yields encoded ids (entry id + quantized frequency). Split them the way the stored
                // path does in DecodeAndSave, otherwise the ids never match and the frequencies stay zero - leaving
                // every document with the score buffer's initial value.
                EntryIdEncodings.Decode(bm25.Matches, bm25.Scores);
                CalculateScoreFromMemory(bm25, matches, scores, boostFactor);
                bm25._currentId = bm25._bufferCapacity;
            }
        }

        return new Bm25Relevance(indexSearcher, termFrequency, context, numberOfDocuments, termRatioToWholeCollection, &PostingListCalculateScoreDynamically)
        {
            _setIterator = new PostingList(indexSearcher._transaction.LowLevelTransaction, Slices.Empty, postingList.State).Iterate()
        };
    }
}

/// <summary>
/// Relevance of one term of a multi-term match. A term with a single document is kept inline, without a Bm25Relevance.
/// </summary>
internal struct TermRelevance
{
    public Bm25Relevance Relevance;
    public long EntryId;
    public float Denominator;
    public short Frequency;
}