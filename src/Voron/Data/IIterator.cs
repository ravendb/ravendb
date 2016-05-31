using System;

namespace Voron.Data
{
    public interface IIterator : IDisposable
    {
        bool Seek(Slice key);
        Slice CurrentKey { get; }
        int GetCurrentDataSize();
        Slice RequiredPrefix { get; set; }
        Slice MaxKey { get; set; }
        bool MoveNext();
        bool MovePrev();
        bool DeleteCurrentAndMoveNext();
        bool Skip(int count);
        ValueReader CreateReaderForCurrent();

        event Action<IIterator> OnDisposal;
    }
}
