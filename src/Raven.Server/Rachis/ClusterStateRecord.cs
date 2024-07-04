using Raven.Client.ServerWide;

namespace Raven.Server.Rachis;

public record ClusterStateRecord(
    RachisState State,
    long Term,
    char DefaultIdentityPartsSeparator
    )
{
    public static ClusterStateRecord Empty = new ClusterStateRecord(RachisState.Passive, -1, Client.Constants.Identities.DefaultSeparator);
}
