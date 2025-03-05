using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session;

public interface IAttachmentsSessionOperationsBaseOfTheBase
{
    /// <summary>
    /// Returns the attachments info of a document.
    /// </summary>
    AttachmentName[] GetNames(object entity);
}
