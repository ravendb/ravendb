using System;

namespace Voron.Data.CompactTrees;

public interface ITreeIterator
{
    void Init<T>(T parent);
    void Reset();
    bool MoveNext(out long value);
}
