using Voron;

namespace Raven.Server.Documents.BackgroundWork;

public class DocumentExpirationInfo
{
    public Slice Ticks { get; }
    public Slice LowerId { get; }
    public string Id { get; }
    public BackgroundWorkInfoStatus Status { get; set; }

    public DocumentExpirationInfo()
    {
    }

    public DocumentExpirationInfo(Slice ticks, Slice lowerId, string id, BackgroundWorkInfoStatus status)
    {
        Status = status;
        Ticks = ticks;
        LowerId = lowerId;
        Id = id;
    }
}
