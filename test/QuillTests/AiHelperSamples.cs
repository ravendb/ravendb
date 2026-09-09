using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Sparrow.Json;

namespace QuillTests;

/// Shared sample configs and internal-API response envelopes for AI-Helper tests. Envelopes are serialized
/// through RavenDB conventions to match the wire shape the internal service produces.
internal static class AiHelperSamples
{
    public static CdcSinkConfiguration BuildCdcConfig() => new()
    {
        Name = "shop-cdc",
        ConnectionStringName = "src",
        Tables =
        [
            new CdcSinkTableConfig
            {
                CollectionName = "Orders",
                SourceTableSchema = "public",
                SourceTableName = "orders",
                PrimaryKeyColumns = ["id"],
                Columns =
                [
                    new CdcColumnMapping { Column = "id", Name = "Id", Type = CdcColumnType.Default },
                    new CdcColumnMapping { Column = "data", Name = "Data", Type = CdcColumnType.Json },
                ],
                EmbeddedTables =
                [
                    new CdcSinkEmbeddedTableConfig
                    {
                        SourceTableName = "order_lines",
                        PropertyName = "Lines",
                        Type = CdcSinkRelationType.Array,
                        PrimaryKeyColumns = ["line_id"],
                        JoinColumns = ["order_id"],
                        Columns = [new CdcColumnMapping { Column = "line_id", Name = "LineId", Type = CdcColumnType.Default }],
                    },
                ],
                LinkedTables =
                [
                    new CdcSinkLinkedTableConfig
                    {
                        SourceTableName = "customers",
                        PropertyName = "Customer",
                        JoinColumns = ["customer_id"],
                        LinkedCollectionName = "Customers",
                    },
                ],
            },
        ],
        Postgres = new CdcSinkPostgresSettings { PublicationName = "pub", SlotName = "slot" },
    };

    public static AiAgentConfiguration BuildAgentConfig() => new()
    {
        Identifier = "shop-assistant",
        Name = "Shop Assistant",
        ConnectionStringName = "gpt",
        SystemPrompt = "You help shoppers find orders.",
        Queries = [new AiAgentToolQuery { Name = "findOrders", Description = "find orders", Query = "from Orders" }],
        Parameters = [new AiAgentParameter { Name = "customerId", Description = "the customer id" }],
    };

    public static string CdcEnvelope(CdcSinkConfiguration config, string status = "Success", int inputTokens = 11, int outputTokens = 22)
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        var envelope = new
        {
            Status = status,
            Configuration = config,
            Rationale = new[] { "uses the orders table" },
            InputTokenCount = inputTokens,
            OutputTokenCount = outputTokens,
        };
        return DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(envelope, ctx).ToString();
    }

    public static string AgentEnvelope(params AiAgentConfiguration[] agents)
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        var envelope = new
        {
            Status = "Success",
            Configurations = agents,
            Rationale = new[] { "derived from the orders mapping" },
            InputTokenCount = 1,
            OutputTokenCount = 2,
        };
        return DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(envelope, ctx).ToString();
    }
}
