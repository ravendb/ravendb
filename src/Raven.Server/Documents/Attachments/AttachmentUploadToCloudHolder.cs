using System;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Attachments;

public class AttachmentUploadToCloudHolder
{
    public readonly Task UploadTask;
    public readonly AbstractBackgroundWorkStorage.DocumentExpirationInfo Doc;
    public readonly long AttachmentSize;

    public AttachmentUploadToCloudHolder(Task uploadTask, AbstractBackgroundWorkStorage.DocumentExpirationInfo doc, long attachmentSize)
    {
        UploadTask = uploadTask ?? throw new ArgumentNullException(nameof(uploadTask));
        Doc = doc ?? throw new ArgumentNullException(nameof(doc));

        if (attachmentSize < 0)
            throw new ArgumentOutOfRangeException(nameof(attachmentSize), "Attachment size cannot be negative.");

        AttachmentSize = attachmentSize;
    }
}
