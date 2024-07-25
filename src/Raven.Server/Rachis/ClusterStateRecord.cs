using System;
using Raven.Client.ServerWide;

namespace Raven.Server.Rachis;

public record ClusterStateRecord(
    RachisState State,
    DateTime When,
    long Term,
    char DefaultIdentityPartsSeparator
    )
{
    public static ClusterStateRecord Empty = new ClusterStateRecord(RachisState.Passive, DateTime.MinValue, -1, Client.Constants.Identities.DefaultSeparator);
}
