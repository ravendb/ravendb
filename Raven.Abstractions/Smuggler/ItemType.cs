using System;

namespace Raven.Abstractions.Smuggler
{
    [Flags]
    public enum ItemType
    {
        Documents,
        Indexes,
        Attachments,
    }
}