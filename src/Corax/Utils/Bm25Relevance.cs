using System;
using System.Runtime.CompilerServices;
using Sparrow.Extensions;
using Sparrow.Server;
using Voron.Data.PostingLists;

namespace Corax.Utils;

//https://www.researchgate.net/publication/45886647_Integrating_the_Probabilistic_Models_BM25BM25F_into_Lucene

public unsafe struct Bm25Relevance : IDisposable
{
    private readonly delegate*<ref Bm25Relevance, Span<long>, int, void> _processFunc;
    private readonly delegate*<ref Bm25Relevance, Span<long>, Span<float>, float, void> _scoreFunc;


    public const float InitialScoreValue = 1 / 1_000_000f; // In case of BM25 this has no "impact", but we need value bigger than 0 when only document boost is involved
    private const int MaximumDocumentCapacity = MaxSizeOfStorage / (sizeof(long) + sizeof(short));
    private const int MaxSizeOfStorage = 1024 * 1024; //1MB;
    private const float BFactor = 0.25f;
    private const float K1 = 2f;

    //We store sum of all length of terms under specific field. Then we can calculate 
    private readonly float _termRatioToWholeCollection;
    private readonly long* _matchBuffer;
    private readonly short* _scoreBuffer;
    private readonly int _numberOfDocuments;
    private int _currentId;
    private readonly float _idf;
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

    //Requirements: ids are sorted!!!
    private static void CalculateScoreFromMemory(ref Bm25Relevance bm25, Span<long> matches, Span<float> scores, float boostFactor)
    {
        //The score will be almost 0. Don't care.
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

            var weight = frequencies[idOfInner] / ((1 - BFactor) + BFactor * bm25._termRatioToWholeCollection);
            scores[idX] += bm25._idf * weight * boostFactor / (K1 + weight);
        }
    }

    /// <summary>
    /// Creates a copy in memory of current match and remove frequencies from `matches` buffer.
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
