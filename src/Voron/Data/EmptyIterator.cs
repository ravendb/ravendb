using System;
using System.Collections.Generic;
using Voron.Data.BTrees;

namespace Voron.Data
{
    public sealed unsafe class EmptyIterator : IIterator
    {
        public bool Seek(Slice key)
        {
            return false;
        }

        public bool DoRequireValidation
        {
            get { throw new InvalidOperationException("No current page"); }
        }

        public Slice CurrentKey
        {
            get { throw new InvalidOperationException("No current page"); }
        }

        public int GetCurrentDataSize()
        {
            throw new InvalidOperationException("No current page");
        }

        public bool Skip(long count)
        {
            throw new InvalidOperationException("No records");
        }

        public ValueReader CreateReaderForCurrent()
        {
            throw new InvalidOperationException("No current page");
        }


        public event Action<IIterator> OnDisposal;

        public IEnumerable<string> DumpValues()
        {
            yield break;
        }

        public unsafe TreeNodeHeader* Current
        {
            get
            {
                throw new InvalidOperationException("No current page");
            }
        }

        public Slice MaxKey { get; set; }

        public Slice RequiredPrefix
        {
            get;
        }

        public void SetRequiredPrefix(Slice prefix)
        {
        }

        public bool MoveNext()
        {
            return false;
        }

        public bool MovePrev()
        {
            return false;
        }

        public void Dispose()
        {
            var action = OnDisposal;
            if (action != null)
                action(this);
        }
    }
}
