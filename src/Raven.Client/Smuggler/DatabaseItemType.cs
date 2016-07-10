using System;

namespace Raven.Client.Smuggler
{
    [Flags]
    public enum DatabaseItemType
    {
        Documents = 0x1,
        Indexes = 0x2,
        Transformers = 0x4,

        VersioningRevisionDocuments = 0x32,

        RemoveAnalyzers = 0x8000,
    }
}