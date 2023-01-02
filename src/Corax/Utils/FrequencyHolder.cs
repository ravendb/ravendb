using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server;

namespace Corax.Utils;

public unsafe struct FrequencyHolder : IDisposable, IFrequencyHolder
{
    private readonly ByteStringContext _context;
    private IDisposable _memoryHolder;
    private long* _matchBuffer;
    private float* _scoreBuffer;
    private int _currentSize;
    private int _currentId;
    
    public long Count => _currentId;

    public Span<long> Matches => new(_matchBuffer, _currentId);

    public Span<float> Scores => new(_scoreBuffer, _currentId);

    //todo:
    // - we've to find a way to properly handle TermMatches bigger than int32.MAX
    public FrequencyHolder(ByteStringContext context, int initialSize)
    {
        _context = context;
        _currentSize = initialSize;
        _memoryHolder = context.Allocate(initialSize * (sizeof(long) + sizeof(float)), out var buffer);
        _matchBuffer = (long*)buffer.Ptr;
        _scoreBuffer = (float*)(buffer.Ptr + initialSize * sizeof(long));
        _currentId = 0;
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
