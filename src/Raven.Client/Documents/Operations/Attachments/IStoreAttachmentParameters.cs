using System.IO;

namespace Raven.Client.Documents.Operations.Attachments;

public interface IStoreAttachmentParameters
{
    /// <summary>
    /// The name of the attachment.
    /// </summary>
    string Name { get; set; }

    /// <summary>
    /// The stream of the attachment.
    /// </summary>
    Stream Stream { get; set; }

    /// <summary>
    /// The change vector of the attachment for concurrency control.
    /// </summary>
    string ChangeVector { get; set; }

    /// <summary>
    /// The MIME type of the attachment.
    /// </summary>
    string ContentType { get; set; }

    /// <summary>
    /// The date to upload the attachment to cloud.
    /// </summary>
    RetireAttachmentParameters RetireParameters { get; set; }
}