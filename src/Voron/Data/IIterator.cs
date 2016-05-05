using System;

namespace Voron.Data
{
    public interface IIterator : IDisposable
    {
        Slice CurrentKey { get; }
        Slice RequiredPrefix { get; set; }
        Slice MaxKey { get; set; }

        bool Seek(Slice key);
        bool MoveNext();
        bool MovePrev();
        bool Skip(int count);

        ValueReader CreateReaderForCurrent();
        int GetCurrentDataSize();

        event Action<IIterator> OnDisposal;
    }
}
