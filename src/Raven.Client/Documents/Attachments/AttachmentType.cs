namespace Raven.Client.Documents.Attachments
{
    /// <summary>
    /// Specifies the types of attachments that can be retrieved or managed in the database.
    /// </summary>
    /// <remarks>
    /// This enumeration differentiates between document attachments and revision attachments.
    /// </remarks>
    public enum AttachmentType : byte
    {
        /// <summary>
        /// Indicates that the attachment is associated with a document.
        /// </summary>
        Document = 1,

        /// <summary>
        /// Indicates that the attachment is associated with a revision.
        /// </summary>
        Revision = 2
    }
}
