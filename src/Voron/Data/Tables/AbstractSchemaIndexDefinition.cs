using System;
using Sparrow.Server;

namespace Voron.Data.Tables
{
    [Flags]
    public enum TableIndexType
    {
        Default = 0x01,
        BTree = 0x01,
        Custom = 0x2
    }

    public abstract class AbstractSchemaIndexDefinition
    {
        public abstract TableIndexType Type { get; }

        public bool IsGlobal;

        public Slice Name;

        public abstract ByteStringContext.Scope GetSlice(ByteStringContext context, ref TableValueReader value,
            out Slice slice);

        public abstract ByteStringContext.Scope GetSlice(ByteStringContext context, TableValueBuilder value,
            out Slice slice);

        public abstract byte[] Serialize();

        public abstract void Validate(AbstractSchemaIndexDefinition actual);

        public virtual void Validate()
        {
            if (Name.HasValue == false || SliceComparer.Equals(Slices.Empty, Name))
                throw new ArgumentException("Index name must be non-empty", nameof(Name));
        }

    }
}
