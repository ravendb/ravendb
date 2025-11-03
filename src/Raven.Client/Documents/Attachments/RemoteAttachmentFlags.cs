using System;

namespace Raven.Client.Documents.Attachments
{
    [Flags]
    public enum RemoteAttachmentFlags
    {
        None = 0,
        Remote = 0x1
    }
}
