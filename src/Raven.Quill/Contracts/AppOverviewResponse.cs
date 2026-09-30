namespace Raven.Quill.Contracts;

public sealed record AppOverviewResponse(
    string Slug,
    long Documents,
    int ConfiguredAgents,
    int Channels,
    int ActiveChannels);
