using System;

namespace Raven.Server.Documents.Replication.ReplicationItems;

[Flags]
public enum AttachmentTombstoneFlags
{
    FromStorageOnly = 0x7FFFFFFF,


    None = 0
}
