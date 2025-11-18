using Voron;

namespace Raven.Server.Documents.BackgroundWork;

public class AttachmentRemoteInfo : DocumentExpirationInfo
{
    public long AttachmentsSize;

    public AttachmentRemoteInfo()
    {

    }
    public AttachmentRemoteInfo(Slice ticksSlice, Slice docId, string id, BackgroundWorkInfoStatus delete) : base (ticksSlice, docId, id, delete)
    {
    }
}
