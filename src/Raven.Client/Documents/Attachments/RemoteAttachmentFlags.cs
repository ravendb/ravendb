using System;

namespace Raven.Client.Documents.Attachments
{
    /// <summary>
    /// Flags that indicate the location and characteristics of an attachment.
    /// </summary>
    [Flags]
    public enum RemoteAttachmentFlags
    {
        /// <summary>
        /// No flags are set. The attachment is stored locally.
        /// </summary>
        None = 0,
        
        /// <summary>
        /// The attachment is stored remotely in cloud storage rather than in the local database.
        /// </summary>
        Remote = 0x1
    }
}
