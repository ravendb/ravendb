using System;
using System.Runtime.CompilerServices;
using Sparrow.Extensions;
using Sparrow.Server;
using Voron.Data.PostingLists;

namespace Corax.Utils;

//This is implementation of BM25F from this white-paper:
//https://www.researchgate.net/publication/45886647_Integrating_the_Probabilistic_Models_BM25BM25F_into_Lucene

public unsafe struct Bm25Relevance : IDisposable
{
    private readonly delegate*<ref Bm25Relevance, Span<long>, int, void> _processFunc;
    private readonly delegate*<ref Bm25Relevance, Span<long>, Span<float>, float, void> _scoreFunc;

    /// <summary>
    /// The default score array value must be bigger than 0 because of support for document boost.
    /// This is necessary in case we use 'order by score()' without a WHERE clause, where the document boost is the only factor in the equation.
    /// So in order not to multiply by 0 let set it to be very small. BM25F is using sum, so this has no impact on the result. 
    /// </summary>
    public const float InitialScoreValue = 1 / 1_000_000f; 
    
    private const int MaximumDocumentCapacity = MaxSizeOfStorage / (sizeof(long) + sizeof(short));
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


    public readonly bool IsStored;
    public readonly bool IsInitialized;
    private bool _isDisposed;
    private readonly int _bufferCapacity;
    private Span<long> Matches => new(_matchBuffer, _currentId);
    private Span<short> Scores => new(_scoreBuffer, _currentId);

    private Bm25Relevance(IndexSearcher indexSearcher, long termFrequency, ByteStringContext context, int numberOfDocuments, double termRatioToWholeCollection,
        delegate*<ref Bm25Relevance, Span<long>, Span<float>, float, void> dynamicalScoreFunc)
    {
        IsInitialized = true;
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

        context.Allocate(_bufferCapacity * (sizeof(long) + sizeof(short)), out var buffer);
        _matchBuffer = (long*)buffer.Ptr;
        _scoreBuffer = (short*)(buffer.Ptr + _bufferCapacity * sizeof(long));

        _idf = ComputeIdf(indexSearcher, termFrequency);
    }

    /// <summary>
    /// We add 1 to the IDF (Inverse Document Frequency) value to ensure that it is not equal to 0.
    /// This guarantees that the boost factor is not 'forgotten' in the calculation of the score. 
    /// </summary>
    private static float ComputeIdf(IndexSearcher indexSearcher, long termFrequency)
    {
        var m = indexSearcher.NumberOfEntries - termFrequency + 0.5D;
        var d = termFrequency + 0.5D;

        return (float)Math.Log((m / d) + 1);
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (_isDisposed)
            ThrowAlreadyDisposed();

        _scoreFunc(ref this, matches, scores, boostFactor);
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
    private static void CalculateScoreFromMemory(ref Bm25Relevance bm25, Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (bm25._idf.AlmostEquals(0f))
            return;

        var innerItems = bm25.Matches;
        var frequencies = bm25.Scores;

        for (int idX = 0; idX < matches.Length; ++idX)
        {
            var entryId = matches[idX];
            var idOfInner = innerItems.BinarySearch(entryId);

            if (idOfInner < 0)
                continue;

            var weight = frequencies[idOfInner] * boostFactor / ((1 - BFactor) + BFactor * bm25._termRatioToWholeCollection);
            scores[idX] += bm25._idf * weight  / (K1 + weight);
        }
    }

    /// <summary>
    /// Returns decoded spans of ids.
    /// </summary>
    public void Process(Span<long> matches, int count) => _processFunc(ref this, matches, count);

    private static void DecodeAndDiscard(ref Bm25Relevance bm25, Span<long> matches, int count)
    {
        EntryIdEncodings.DecodeAndDiscardFrequency(matches, count);
    }

    private static void DecodeAndSave(ref Bm25Relevance bm25, Span<long> matches, int count)
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
            return EntryIdEncodings.Decode(entry).EntryId;

        (*(_matchBuffer + _currentId), *(_scoreBuffer + _currentId)) = EntryIdEncodings.Decode(entry);
        _currentId += 1;

        return *(_matchBuffer + _currentId - 1);
    }

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
    }

    public static Bm25Relevance Once(IndexSearcher indexSearcher, long termFrequency, ByteStringContext context, int numberOfDocuments, double termRatioToWholeCollection)
    {
        return new(indexSearcher, termFrequency, context, numberOfDocuments, termRatioToWholeCollection, dynamicalScoreFunc: null);
    }

    public static Bm25Relevance Small(IndexSearcher indexSearcher, long termFrequency, ByteStringContext context, int numberOfDocuments,
        double termRatioToWholeCollection)
    {
        return new(indexSearcher, termFrequency, context, numberOfDocuments, termRatioToWholeCollection, dynamicalScoreFunc: null);
    }

    public static Bm25Relevance Set(IndexSearcher indexSearcher, long termFrequency, ByteStringContext context, int numberOfDocuments, double termRatioToWholeCollection,
        PostingList postingList)
    {
        static void PostingListCalculateScoreDynamically(ref Bm25Relevance bm25, Span<long> matches, Span<float> scores, float boostFactor)
        {
            bm25._currentId = bm25._bufferCapacity;
            while (bm25._setIterator.Fill(bm25.Matches, out var read, pruneGreaterThanOptimization: matches[^1]) && read > 0)
            {
                bm25._currentId = read;
                CalculateScoreFromMemory(ref bm25, matches, scores, boostFactor);
                bm25._currentId = bm25._bufferCapacity;
            }
        }

        return new Bm25Relevance(indexSearcher, termFrequency, context, numberOfDocuments, termRatioToWholeCollection, &PostingListCalculateScoreDynamically)
        {
            _setIterator = postingList.Iterate()
        };
    }
}
