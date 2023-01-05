using System;
using System.Runtime.CompilerServices;
using Sparrow.Server;

namespace Corax.Utils;

public unsafe struct Bm25 : IDisposable
{
    public const float BFactor = 0.25f;
    public const float Bias = 0.75f;
    public const float K1 = 2f;
    public const float LcAvlC = .7f; //todo
    
    private readonly ByteStringContext _context;
    private IDisposable _memoryHolder;
    private long* _matchBuffer;
    private float* _scoreBuffer;
    private int _currentSize;
    private int _currentId;

    private readonly float _idf;
    
    
    public long Count => _currentId;

    public Span<long> Matches => new(_matchBuffer, _currentId);

    public Span<float> Scores => new(_scoreBuffer, _currentId);

    //todo:
    // - we've to find a way to properly handle TermMatches bigger than int32.MAX
    public Bm25(IndexSearcher indexSearcher, long termFrequency, ByteStringContext context, int initialSize)
    {
        _context = context;
        _currentSize = initialSize;
        _memoryHolder = context.Allocate(initialSize * (sizeof(long) + sizeof(float)), out var buffer);
        _matchBuffer = (long*)buffer.Ptr;
        _scoreBuffer = (float*)(buffer.Ptr + initialSize * sizeof(long));
        _currentId = 0;
        _idf = ComputeIdf(indexSearcher, termFrequency);
    }

    private static float ComputeIdf(IndexSearcher indexSearcher, long termFrequency)
    {
        var m = indexSearcher.NumberOfEntries - termFrequency + 0.5D;
        var d = termFrequency + 0.5D;

        return (float)Math.Log(m / d);
    }

    public void Score(Span<long> matches, Span<float> scores)
    {
        //todo add some threshold when it is worth to sort :)
        MemoryExtensions.Sort(Matches, Scores);
        
        var innerItems = Matches;
        var frequencies = Scores;
        
        for (int idX = 0; idX < matches.Length; ++idX)
        {
            var entryId = matches[idX];
            var idOfInner = innerItems.BinarySearch(entryId);
            
            if (idOfInner < 0)
                continue;

            var weight = frequencies[idOfInner] / ((1 - BFactor) + BFactor * LcAvlC);
            scores[idX] += _idf * weight / (K1 + weight);
        }
    }
    
    private void UnlikelyGrow(int count)
    {
        int newSize = (int)(_currentSize * 1.5);
        if (newSize - _currentId <= count)
            newSize += (int)(count * 1.5);

        var memoryHolder = _context.Allocate(newSize * (sizeof(long) + sizeof(float)), out var buffer);
        long* newMatchBuffer = (long*)buffer.Ptr;
        float* newScoreBuffer = (float*)(buffer.Ptr + sizeof(long) * newSize);

        Unsafe.CopyBlockUnaligned(newMatchBuffer, _matchBuffer, sizeof(long) * (uint)_currentId);
        Unsafe.CopyBlockUnaligned(newScoreBuffer, _scoreBuffer, sizeof(float) * (uint)_currentId);

        _matchBuffer = newMatchBuffer;
        _scoreBuffer = newScoreBuffer;
        _currentSize = newSize;
        _memoryHolder.Dispose();
        _memoryHolder = memoryHolder;
    }
    
    /// <summary>
    /// Creates a copy in memory of current match and remove frequencies from `matches` buffer.
    /// </summary>
    public void Process(Span<long> matches, int count)
    {
        if (_currentSize - _currentId < count)
            UnlikelyGrow(count);

        FrequencyUtils.DecodeBulk(matches.Slice(0, count), 
            new(_matchBuffer + _currentId, _currentSize - _currentId), 
            new(_scoreBuffer + _currentId, _currentSize- _currentId));
        _currentId += count;
    }

    public long Add(long entry)
    {
        if (_currentId >= _currentSize)
            UnlikelyGrow(_currentSize);
        
        (*(_matchBuffer + _currentId), *(_scoreBuffer + _currentId)) = FrequencyUtils.Decode(entry);
        _currentId += 1;
        
        return *(_matchBuffer + _currentId - 1);
    }
    
    public void Dispose()
    {
        _memoryHolder?.Dispose();
    }

    public void Remove()
    {
        _currentId -= 1;
    }
}
