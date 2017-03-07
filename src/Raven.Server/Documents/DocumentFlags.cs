using System;

namespace Raven.Server.Documents
{
    [Flags]
    public enum DocumentFlags
    {
        None = 0,

        Artificial = 0x1,
        Versioned = 0x2,
        SkipVersioning = 0x4,
        ForceVersioning = 0x8,

        FromIndex = 0x10,
        FromVersionStorage = 0x20,
        FromReplication = 0x40,
        HasAttachments = 0x80,
    }
}