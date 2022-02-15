using System;
using Sparrow.Server;

namespace Voron.Data.Tables
{
    [Flags]
    public enum TableIndexType
    {
        BTree = 0x01,
        Dynamic = 0x2
    }

    public abstract class AbstractBTreeIndexDef
    {
        public abstract TableIndexType Type { get; }

        public bool IsGlobal;

        public Slice Name;

        public abstract ByteStringContext.Scope GetSlice(ByteStringContext context, ref TableValueReader value,
            out Slice slice);

        public abstract ByteStringContext.Scope GetSlice(ByteStringContext context, TableValueBuilder value,
            out Slice slice);

        public abstract byte[] Serialize();

        public abstract void Validate(AbstractBTreeIndexDef actual);

        public virtual void Validate()
        {
            if (Name.HasValue == false || SliceComparer.Equals(Slices.Empty, Name))
                throw new ArgumentException("Index name must be non-empty", nameof(Name));
        }

    }
}
