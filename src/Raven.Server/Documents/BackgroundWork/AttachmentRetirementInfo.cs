using Voron;

namespace Raven.Server.Documents.BackgroundWork;

public class AttachmentRetirementInfo : BackgroundWorkInfo
{
    public Slice Key => TreeKey;
    public string RemoteIdentifier => Identifier;

    public AttachmentRetirementInfo()
    {
    }

    public AttachmentRetirementInfo(Slice ticks, Slice attachmentKey, string retirementIdentifier, DocumentExpirationInfoStatus status)
        : base(ticks, attachmentKey, retirementIdentifier, status)
    {
    }

    public override string GetIdentifier()
    {
        return RemoteIdentifier;
    }

    public override Slice GetTreeKey()
    {
        return Key;
    }
}
