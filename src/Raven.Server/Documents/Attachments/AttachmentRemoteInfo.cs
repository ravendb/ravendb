using System;
using System.Collections.Generic;
using Raven.Server.Documents.BackgroundWork;

namespace Raven.Server.Documents.Attachments;

public class AttachmentRemoteInfo
{
    public List<string> DocumentIds = new List<string>();

    public BackgroundWorkInfoStatus Status = BackgroundWorkInfoStatus.Process;

    public AttachmentUploader AttachmentUploader;

    public string Hash;

    public long RetryCount;

    public AttachmentRemoteInfo()
    {

    }

    public Exception Exception { get; set; }
}
