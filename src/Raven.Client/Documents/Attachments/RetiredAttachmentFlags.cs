using System;

namespace Raven.Client.Documents.Attachments
{
    [Flags]
    public enum RetiredAttachmentFlags
    {
        None = 0,
        Retired = 0x1
    }
}
