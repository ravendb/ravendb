using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.AiAppliance.Wizard;

public sealed class MapRequest
{
    public long? TaskId { get; init; }

    public bool? Disabled { get; set; }

    public string Name { get; set; } = string.Empty;

    public string MentorNode { get; init; } = string.Empty;

    public bool? PinToMentorNode { get; init; }

    public string ConnectionStringName { get; init; } = string.Empty;

    public required List<CdcSinkTableConfig> Tables { get; init; } = [];

    public CdcSinkPostgresSettings? Postgres { get; init; }

    public bool? SkipInitialLoad { get; init; }

    public CdcSinkConfiguration ToClientConfiguration() => new()
    {
        TaskId = TaskId ?? 0,
        Disabled = Disabled ?? false,
        Name = Name,
        MentorNode = MentorNode,
        PinToMentorNode = PinToMentorNode ?? false,
        ConnectionStringName = ConnectionStringName,
        Tables = Tables ?? [],
        Postgres = Postgres,
        SkipInitialLoad = SkipInitialLoad ?? false,
    };
}
