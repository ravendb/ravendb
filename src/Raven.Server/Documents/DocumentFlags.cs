using System;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Smuggler;

namespace Raven.Server.Documents
{
    [Flags]
    public enum DocumentFlags
    {
        None = 0,

        Artificial = 0x1,
        HasRevisions = 0x2,
        DeleteRevision = 0x4,

        // The revision generated from an old document is a special case and will be always replicated. (relevant when creating revision config for existing docs)
        FromOldDocumentRevision = 0x8,

        FromIndex = 0x10,
        Revision = 0x20,
        FromReplication = 0x40,
        Reserved3 = 0x80,

        HasAttachments = 0x100,
        Resolved = 0x200,
        Conflicted = 0x400,
        HasCounters = 0x800,

        FromClusterTransaction = 0x1000,
        Reverted = 0x2000,

        HasTimeSeries = 0x4000,

        ForceCreated = 0x10000 // 0x8000 is already taken in 6.0 branch
    }

    [Flags]
    public enum NonPersistentDocumentFlags
    {
        None = 0,

        LegacyRevision = 0x1,
        LegacyHasRevisions = 0x2,
        FromSmuggler = 0x4,
        FromReplication = 0x8,
        ByAttachmentUpdate = 0x10,
        ResolveAttachmentsConflict = 0x20,
        SkipRevisionCreation = 0x40,
        Resolved = 0x80,
        SkipRevisionCreationForSmuggler = 0x100,
        ResolveCountersConflict = 0x200,
        ByCountersUpdate = 0x400,
        FromResolver = 0x800,
        ByEnforceRevisionConfiguration = 0x1000,
        ResolveTimeSeriesConflict = 0x2000,
        ByTimeSeriesUpdate = 0x4000,
        LegacyDeleteMarker = 0x8000,
        ForceRevisionCreation = 0x10000,
        AllowDataAsNull = 0x20000
    }

    public static class EnumExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contain(this DocumentFields current, DocumentFields flag)
        {
            return (current & flag) == flag;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contain(this DocumentFlags current, DocumentFlags flag)
        {
            return (current & flag) == flag;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DocumentFlags Strip(this DocumentFlags current, DocumentFlags flag)
        {
            return current & ~flag;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contain(this NonPersistentDocumentFlags current, NonPersistentDocumentFlags flag)
        {
            return (current & flag) == flag;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contain(this DatabaseRecordItemType current, DatabaseRecordItemType flag)
        {
            return (current & flag) == flag;
        }
    }
}
