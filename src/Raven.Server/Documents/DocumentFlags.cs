using System;

namespace Raven.Server.Documents
{
    [Flags]
    public enum DocumentFlags
    {
        None = 0,
        Versioned = 0x1,
        Artificial = 0x2,
        FromIndex = 0x4,
        FromVersionStorage = 0x8,
        FromReplication = 0x10,
        HasAttachments = 0x20,
    }
}