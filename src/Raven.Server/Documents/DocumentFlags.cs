using System;

namespace Raven.Server.Documents
{
    [Flags]
    public enum DocumentFlags
    {
        None = 0,

        Artificial = 0x1,
        Versioned = 0x2,
        Revision = 0x4,
        Reserved = 0x8,

        FromIndex = 0x10,
        Reserved1 = 0x20,
        FromReplication = 0x40,
        FromSmuggler = 0x80,

        HasAttachments = 0x100,
        Reserved2 = 0x200,
        LegacyVersioned = 0x300,
        LegacyRevision = 0x400,
    }
}