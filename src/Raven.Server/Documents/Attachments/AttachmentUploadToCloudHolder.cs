using System.Threading.Tasks;

namespace Raven.Server.Documents.Attachments;

public class AttachmentUploadToCloudHolder
{
    public readonly Task UploadTask;
    public readonly AbstractBackgroundWorkStorage.DocumentExpirationInfo Doc;
    public readonly long AttachmentSize;

    public AttachmentUploadToCloudHolder(Task uploadTask, AbstractBackgroundWorkStorage.DocumentExpirationInfo doc, long attachmentSize)
    {
        UploadTask = uploadTask;
        Doc = doc;
        AttachmentSize = attachmentSize;
    }
}
