using System;

namespace Raven.Server.Documents
{
    [Flags]
    public enum InitializeOptions
    {
        None = 0,

        GenerateNewDatabaseId = 1,
        SkipLoadingDatabaseRecord = 2
    }
}