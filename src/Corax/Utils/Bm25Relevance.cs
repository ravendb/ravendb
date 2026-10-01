using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Sparrow.Extensions;
using Sparrow.Server;
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
    private const int MaxSizeOfStorage = 1024 * 1024; //1MB;
    private const float BFactor = 0.25f;
    private const float K1 = 2f;
    private const int GallopRatio = 32;

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
            _processFunc = &DecodeAndDiscard;
            _bufferCapacity = MaximumDocumentCapacity;
            _currentId = MaximumDocumentCapacity;
        }
        else
        {
            _processFunc = &DecodeAndSave;
            _scoreFunc = &CalculateScoreFromMemory;
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

        // Both sides are sorted, so every lookup continues from the previous position.
        if (innerItems.Length < matches.Length)
        {
            var gallop = matches.Length <= GallopRatio * innerItems.Length;
            var idOfMatch = 0;
            for (int idX = 0; idX < innerItems.Length; ++idX)
            {
                idOfMatch = FindLowerBound(matches, idOfMatch, innerItems[idX], gallop);
                if (idOfMatch == matches.Length)
                    return;

                if (matches[idOfMatch] != innerItems[idX])
                    continue;

                scores[idOfMatch] += Contribution(frequencies[idX], boostFactor, denominator, bm25._idf);
            }

            return;
        }

        var gallopInInner = innerItems.Length <= GallopRatio * matches.Length;
        var idOfInner = 0;
        for (int idX = 0; idX < matches.Length; ++idX)
        {
            idOfInner = FindLowerBound(innerItems, idOfInner, matches[idX], gallopInInner);
            if (idOfInner == innerItems.Length)
                return;

            if (innerItems[idOfInner] != matches[idX])
                continue;

            scores[idX] += Contribution(frequencies[idOfInner], boostFactor, denominator, bm25._idf);
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
            _setIterator = postingList.Iterate()
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
