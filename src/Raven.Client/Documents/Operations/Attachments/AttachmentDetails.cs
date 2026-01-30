namespace Raven.Client.Documents.Operations.Attachments
{
    /// <summary>
    /// Contains details about an attachment, including its change vector and associated document ID.
    /// </summary>
    /// <remarks>
    /// This class inherits from <see cref="AttachmentName"/> and provides additional metadata
    /// necessary for managing attachment operations.
    /// </remarks>
    public class AttachmentDetails : AttachmentName
    {
        /// <summary>
        /// The change vector of the attachment for concurrency control.
        /// </summary>
        public string ChangeVector;

        /// <summary>
        /// The ID of the document associated with the attachment.
        /// </summary>
        public string DocumentId;
    }
}
