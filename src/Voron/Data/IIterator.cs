using System;

namespace Voron.Data
{
    public interface IIterator : IDisposable
    {
        bool DoRequireValidation { get; }

        Slice CurrentKey { get; }
        Slice RequiredPrefix { get; }
        Slice MaxKey { get; set; }

        bool Seek(Slice key);
        bool MoveNext();
        bool MovePrev();
        bool Skip(long count);
        void SetRequiredPrefix(Slice prefix);

        ValueReader CreateReaderForCurrent();
        int GetCurrentDataSize();

        event Action<IIterator> OnDisposal;
    }
}
