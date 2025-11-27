using System.Collections.Generic;
using Raven.Server.Documents.BackgroundWork;

namespace Raven.Server.Documents.Attachments;

public class AttachmentRemoteInfo
{
    public long AttachmentsSize = 0L;

    public List<string> DocumentIds = new List<string>();

    public BackgroundWorkInfoStatus Status = BackgroundWorkInfoStatus.Process;

    public AttachmentRemoteInfo()
    {

    }
}
