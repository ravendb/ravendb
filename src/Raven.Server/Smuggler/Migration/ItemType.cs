using System;

namespace Raven.Server.Smuggler.Migration
{
    [Flags]
    public enum ItemType
    {
        None = 0,
        Documents = 1,
        Indexes = 2,
        Attachments = 4,
        RemoveAnalyzers = 32768
    }
}
