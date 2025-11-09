using Voron;

namespace Raven.Server.Documents.BackgroundWork;

public class DocumentExpirationInfo : BackgroundWorkInfo
{
    public Slice LowerId => TreeKey;
    public string Id => Identifier;

    public DocumentExpirationInfo()
    {
    }

    public DocumentExpirationInfo(Slice ticks, Slice documentLowerId, string documentId, BackgroundWorkInfoStatus status)
        : base(ticks, documentLowerId, documentId, status)
    {
    }

    public override string GetIdentifier()
    {
        return Id;
    }

    public override Slice GetTreeKey()
    {
        return LowerId;
    }
}
