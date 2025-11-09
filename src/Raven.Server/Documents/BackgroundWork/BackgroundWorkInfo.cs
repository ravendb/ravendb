using Voron;

namespace Raven.Server.Documents.BackgroundWork;

public abstract class BackgroundWorkInfo
{
    public Slice Ticks { get; }
    protected Slice TreeKey { get; }
    protected string Identifier { get; }
    public BackgroundWorkInfoStatus Status { get; set; }

    public abstract string GetIdentifier();
    public abstract Slice GetTreeKey();

    internal BackgroundWorkInfo()
    {
    }

    protected BackgroundWorkInfo(Slice ticks, Slice treeKey, string identifier, BackgroundWorkInfoStatus status)
    {
        Status = status;
        Ticks = ticks;
        TreeKey = treeKey;
        Identifier = identifier;
    }
}
