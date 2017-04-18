using System;

namespace Raven.Server.Documents
{
    [Flags]
    public enum DocumentFlags
    {
        None = 0,

        Artificial = 0x1,
        Versioned = 0x2,
        Reserved1 = 0x4,
        Reserved2 = 0x8,

        FromIndex = 0x10,
        Revision = 0x20,
        FromReplication = 0x40,
        Reserved3 = 0x80,

        HasAttachments = 0x100
    }

    [Flags]
    public enum NonPersistentDocumentFlags
    {
        None = 0,

        LegacyRevision = 0x1,
        LegacyVersioned = 0x2,
        FromSmuggler = 0x4,
        FromReplication = 0x8,
        ByAttachmentUpdate = 0x10,
        ResolvedAttachmentConflict = 0x20
    }
}