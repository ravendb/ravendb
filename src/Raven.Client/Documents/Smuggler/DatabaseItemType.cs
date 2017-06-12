using System;

namespace Raven.Client.Documents.Smuggler
{
    [Flags]
    public enum DatabaseItemType
    {
        None = 0,

        Documents = 1 << 0,
        RevisionDocuments = 1 << 1,
        Indexes = 1 << 2,
        Transformers = 1 << 3,
        LocalIdentities = 1 << 4,
        ClusterIdentities = 1 << 5,
    }
}