using System;

namespace Voron.Data.CompactTrees;

public interface ICompactTreeIterator
{
    void Init(CompactTree tree);
    void Seek(ReadOnlySpan<byte> key);
    void Seek(CompactKey key);
    void Reset();
    bool Skip(long count);

    bool MoveNext(out CompactKeyCacheScope scope, out long value);
}
