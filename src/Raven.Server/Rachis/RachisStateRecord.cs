using Raven.Client.ServerWide;

namespace Raven.Server.Rachis;

public record RachisStateRecord(
    RachisState State,
    long Term
    )
{
    public static RachisStateRecord Empty = new RachisStateRecord(RachisState.Passive, -1);
}
