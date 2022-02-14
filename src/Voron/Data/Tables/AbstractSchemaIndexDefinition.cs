using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Impl;

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
        public abstract TableIndexType Type { get;}

        public bool IsGlobal;

        public Slice Name;

        public abstract ByteStringContext.Scope GetSlice(ByteStringContext context, ref TableValueReader value,
            out Slice slice);

        public abstract ByteStringContext.Scope GetSlice(ByteStringContext context, TableValueBuilder value,
            out Slice slice);

        public abstract byte[] Serialize();

        public abstract void Validate(AbstractSchemaIndexDefinition actual);

    }

}
