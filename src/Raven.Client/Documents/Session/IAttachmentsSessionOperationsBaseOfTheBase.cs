using Raven.Client.Documents.Operations.Attachments;

namespace Raven.Client.Documents.Session;
//TODO: egor this needs to be removed
public interface IAttachmentsSessionOperationsBaseOfTheBase
{
    /// <summary>
    /// Returns the attachments info of a document.
    /// </summary>
    AttachmentName[] GetNames(object entity);
}
