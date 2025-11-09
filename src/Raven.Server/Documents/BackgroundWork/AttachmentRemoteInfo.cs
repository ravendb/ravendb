using Voron;

namespace Raven.Server.Documents.BackgroundWork;

public class AttachmentRemoteInfo : BackgroundWorkInfo
{
    public Slice Key => TreeKey;
    public string DestinationIdentifier => Identifier;
    public long Size;
    public AttachmentRemoteInfo()
    {
    }

    public AttachmentRemoteInfo(Slice ticks, Slice attachmentKey, string destinationIdentifier, BackgroundWorkInfoStatus status)
        : base(ticks, attachmentKey, destinationIdentifier, status)
    {
    }

    public override string GetIdentifier()
    {
        return DestinationIdentifier;
    }

    public override Slice GetTreeKey()
    {
        return Key;
    }
}
