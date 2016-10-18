using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace Raven.Bundles.UniqueConstraints
{
    using Abstractions.Data;
    using Database.Plugins;
    using Json.Linq;
    using Raven.Database.Extensions;
    using System;

    [InheritedExport(typeof(AbstractDeleteTrigger))]
    [ExportMetadata("Bundle", "Unique Constraints")]
    [ExportMetadata("IsRavenExternalBundle", true)]
    public class UniqueConstraintsDeleteTrigger : AbstractDeleteTrigger
    {
        public override void OnDelete(string key, TransactionInformation transactionInformation)
        {
            if (key.StartsWith("Raven"))
                return;

            var doc = Database.Documents.Get(key, transactionInformation);

            if (doc == null)
                return;

            var metadata = doc.Metadata;

            var entityName = metadata.Value<string>(Constants.RavenEntityName) + "/";

            var uniqueConstraits = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

            if (uniqueConstraits == null)
                return;

            foreach (var property in uniqueConstraits)
            {
                var constraint = Util.GetConstraint(property);
                var prefix = "UniqueConstraints/" + entityName + constraint.PropName+ "/"; // UniqueConstraints/EntityNamePropertyName/
                var prop = doc.DataAsJson[constraint.PropName];

                string[] uniqueValues;
                if (!Util.TryGetUniqueValues(prop, out uniqueValues))
                    continue;

                foreach (var uniqueValue in uniqueValues)
                {
                    var escapedUniqueValue = Util.EscapeUniqueValue(uniqueValue, constraint.CaseInsensitive);
                    var uniqueConstraintsDocumentKey = prefix + escapedUniqueValue;
                    var uniqueConstraintsDocument = Database.Documents.Get(uniqueConstraintsDocumentKey, transactionInformation);

                    if (uniqueConstraintsDocument == null)
                        continue;

                    var removed = RemoveConstraintFromUniqueConstraintDocument(uniqueConstraintsDocument, escapedUniqueValue);

                    if (ShouldRemoveUniqueConstraintDocument(uniqueConstraintsDocument))
                    {
                        Database.Documents.Delete(uniqueConstraintsDocumentKey, null, transactionInformation);
                    }
                    else if (removed)
                    {
                        Database.Documents.Put(
                            uniqueConstraintsDocumentKey,
                            null,
                            uniqueConstraintsDocument.DataAsJson,
                            uniqueConstraintsDocument.Metadata,
                            transactionInformation);
                    }
                }
            }
        }

        private static bool ShouldRemoveUniqueConstraintDocument(JsonDocument uniqueConstraintsDocument)
        {
            if (!uniqueConstraintsDocument.DataAsJson.ContainsKey("Constraints"))
                return true;

            if (uniqueConstraintsDocument.DataAsJson.Keys.Count == 0)
                return true;

            var constraints = (RavenJObject)uniqueConstraintsDocument.DataAsJson["Constraints"];

            if (constraints.Keys.Count == 0)
                return true;

            return false;
        }

        private static bool RemoveConstraintFromUniqueConstraintDocument(JsonDocument uniqueConstraintsDocument, string escapedUniqueValue)
        {
            if (!uniqueConstraintsDocument.DataAsJson.ContainsKey("Constraints"))
                return false;

            var constraints = (RavenJObject)uniqueConstraintsDocument.DataAsJson["Constraints"];

            return constraints.Remove(escapedUniqueValue);
        }
    }
}
