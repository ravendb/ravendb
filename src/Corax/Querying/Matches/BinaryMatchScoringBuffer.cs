using System;
using System.Runtime.InteropServices;
using Sparrow.Server;

namespace Corax.Querying.Matches;

// A BinaryMatch can see matches that the AND did not return, so we must exclude them to calculate the actual scores.
internal ref struct BinaryMatchScoringBuffer : IDisposable
{
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;
    private readonly Span<long> _matches;
    private readonly Span<int> _positions;
    private readonly Span<float> _scores;

    public int Count { get; private set; }

    public Span<long> Matches => _matches[..Count];

    public Span<float> Scores => _scores[..Count];

    public BinaryMatchScoringBuffer(ByteStringContext allocator, int count)
    {
        _scope = allocator.Allocate(count * (sizeof(long) + sizeof(int) + sizeof(float)), out var buffer);
        var bytes = buffer.ToSpan();
        _matches = MemoryMarshal.Cast<byte, long>(bytes[..(count * sizeof(long))]);
        _positions = MemoryMarshal.Cast<byte, int>(bytes.Slice(count * sizeof(long), count * sizeof(int)));
        _scores = MemoryMarshal.Cast<byte, float>(bytes.Slice(count * (sizeof(long) + sizeof(int)), count * sizeof(float)));
        _scores.Clear();
    }

    public void Add(long id, int position)
    {
        _matches[Count] = id;
        _positions[Count] = position;
        Count++;
    }

    public void AddScoresTo(Span<float> scores)
    {
        for (var i = 0; i < Count; i++)
            scores[_positions[i]] += _scores[i];
    }

    public void SaveScores(Span<float> scores)
    {
        for (var i = 0; i < Count; i++)
            _scores[i] = scores[_positions[i]];
    }

    public void RestoreScores(Span<float> scores)
    {
        for (var i = 0; i < Count; i++)
            scores[_positions[i]] = _scores[i];
    }

    public void Dispose() => _scope.Dispose();
}
