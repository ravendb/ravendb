using System;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Attribution.Data;
using Raven.Database.Plugins;
using Raven.Http;
using Raven.Json.Linq;

namespace Raven.Bundles.Attribution.Triggers
{
    public class AttributionPutTrigger : AbstractPutTrigger
    {
        public override void OnPut(string key, RavenJObject document, RavenJObject metadata, Abstractions.Data.TransactionInformation transactionInformation)
        {
            // Exclude RavenDB internal documents.
            if (key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
                return;

            // Check the attribution bundle configuration document to determine if this document type is excluded.
            var attributionConfiguration = GetDocumentAttributionConfiguration(metadata);
            if (attributionConfiguration.Exclude)
                return;

            // Add author metadata to the document.
            using (Database.DisableAllTriggersForCurrentThread())
            {
                var user = CurrentOperationContext.Headers.Value[Constants.RavenAttributionUser];
                if (user != null)
                    metadata[Constants.RavenDocumentAuthor] = RavenJToken.FromObject(user);
            }
        }

        private AttributionConfiguration GetDocumentAttributionConfiguration(RavenJObject metadata)
        {
            var attributionConfiguration = new AttributionConfiguration
            {
                Exclude = false
            };

            var entityName = metadata.Value<string>("Raven-Entity-Name");
            if (entityName != null)
            {
                var doc = Database.Get("Raven/Attribution/" + entityName, null) ??
                          Database.Get("Raven/Attribution/DefaultConfiguration", null);
                if (doc != null)
                {
                    attributionConfiguration = doc.DataAsJson.JsonDeserialization<AttributionConfiguration>();
                }
            }

            return attributionConfiguration;
        }
    }
}
