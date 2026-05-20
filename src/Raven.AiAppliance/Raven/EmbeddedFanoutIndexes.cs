using Raven.AiAppliance.Schema;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;

namespace Raven.AiAppliance.Raven;

/// Iterates the schema's fanout indexes and `PUT`s them through the maintenance
/// API. No-op when the schema declares none — matches T-1's DemoAgentSchema.
/// The nopcommerce-demo template had three hard-coded indexes for Orders.Items,
/// Customers.ShoppingCart, Products.StockHistory; those move into per-schema
/// definitions and the registrar stays neutral.
public static class EmbeddedFanoutIndexes
{
    public static async Task EnsureAsync(IDocumentStore store, IAgentSchema schema, CancellationToken ct = default)
    {
        if (schema.FanoutIndexes.Count == 0)
            return;

        var defs = schema.FanoutIndexes
            .Select(i => new IndexDefinition
            {
                Name = i.Name,
                Maps = { i.MapExpression },
            })
            .ToArray();

        await store.Maintenance.SendAsync(new PutIndexesOperation(defs), ct);
    }
}
