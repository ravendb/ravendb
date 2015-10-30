// -----------------------------------------------------------------------
//  <copyright file="StructReadResult.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Voron
{
    public class StructReadResult<T>
    {
        public StructReadResult(StructureReader<T> reader, ushort version)
        {
            Reader = reader;
            Version = version;
        }

        public StructureReader<T> Reader { get; private set; }

        public ushort Version { get; private set; }
    }
}
