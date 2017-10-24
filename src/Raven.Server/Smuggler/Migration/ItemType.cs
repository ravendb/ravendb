using System;

namespace Raven.Server.Smuggler.Migration
{
    [Flags]
    public enum ItemType
    {
        Documents = 1,
        Indexes = 2,
        Attachments = 4,
        RemoveAnalyzers = 32768
    }
}
