using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;

using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.UniqueConstraints
{
    [InheritedExport(typeof(AbstractPutTrigger))]
    [ExportMetadata("Bundle", "Unique Constraints")]
    [ExportMetadata("IsRavenExternalBundle", true)]
    public class UniqueConstraintsPutTrigger : AbstractPutTrigger
    {
        public override void AfterPut(string key, RavenJObject document, RavenJObject metadata, Etag etag, TransactionInformation transactionInformation)
        {
            if (key.StartsWith("Raven/"))
            {
                return;
            }

            var entityName = metadata.Value<string>(Constants.RavenEntityName) + "/";

            var properties = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

            if (properties == null || properties.Length <= 0)
                return;

            foreach (var property in properties)
            {
                var constraint = Util.GetConstraint(property);
                var prop = document[constraint.PropName];

                string[] uniqueValues;
                if (!Util.TryGetUniqueValues(prop, out uniqueValues))
                    continue;

                var prefix = "UniqueConstraints/" + entityName + constraint.PropName + "/";

                foreach (var uniqueValue in uniqueValues)
                {
                    var escapedUniqueValue = Util.EscapeUniqueValue(uniqueValue, constraint.CaseInsensitive);
                    var uniqueConstraintsDocumentKey = prefix + escapedUniqueValue;
                    var uniqueConstraintsDocument = Database.Documents.Get(uniqueConstraintsDocumentKey, transactionInformation);                    

                    if (uniqueConstraintsDocument != null)
                    {
                        uniqueConstraintsDocument = DeepCloneDocument(uniqueConstraintsDocument);
                        ConvertUniqueConstraintsDocumentIfNecessary(uniqueConstraintsDocument, escapedUniqueValue); // backward compatibility
}
                    else
                        uniqueConstraintsDocument = new JsonDocument();

                    AddConstraintToUniqueConstraintsDocument(uniqueConstraintsDocument, escapedUniqueValue, key);
                    uniqueConstraintsDocument.Metadata[Constants.IsConstraintDocument] = true;

                    Database.Documents.Put(
                        uniqueConstraintsDocumentKey,
                        null,
                        uniqueConstraintsDocument.DataAsJson,
                        uniqueConstraintsDocument.Metadata,
                        transactionInformation);
                }
            }
        }

        public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            if (key.StartsWith("Raven/"))
            {
                return VetoResult.Allowed;
            }

            var entityName = metadata.Value<string>(Constants.RavenEntityName);

            if (string.IsNullOrEmpty(entityName))
            {
                return VetoResult.Allowed;
            }
            entityName += "/";

            var properties = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

            if (properties == null || properties.Length <= 0)
                return VetoResult.Allowed;

            var invalidFields = new StringBuilder();

            foreach (var property in properties)
            {
                var constraint = Util.GetConstraint(property);

                var prefix = "UniqueConstraints/" + entityName + constraint.PropName+ "/";
                var prop = document[constraint.PropName];
                
                string[] uniqueValues;
                if (!Util.TryGetUniqueValues(prop, out uniqueValues))
                    continue;

                foreach (var uniqueValue in uniqueValues)
                {
                    var escapedUniqueValue = Util.EscapeUniqueValue(uniqueValue, constraint.CaseInsensitive);
                    var checkDocKey = prefix + escapedUniqueValue;
                    var checkDoc = Database.Documents.Get(checkDocKey, transactionInformation);

                    if (checkDoc == null)
                        continue;

                    var checkId = GetRelatedIdFromUniqueConstraintsDocument(checkDoc, escapedUniqueValue);

                    if (!string.IsNullOrEmpty(checkId) && checkId != key)
                        invalidFields.Append(constraint.PropName + ", ");
                }
            }

            if (invalidFields.Length > 0)
            {
                invalidFields.Length = invalidFields.Length - 2;
                return VetoResult.Deny("Ensure unique constraint violated for fields: " + invalidFields);
            }

            return VetoResult.Allowed;
        }

        public override void OnPut(string key, RavenJObject jsonReplicationDocument, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            if (key.StartsWith("Raven/"))
            {
                return;
            }

            var entityName = metadata.Value<string>(Constants.RavenEntityName) + "/";

            var properties = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

            if (properties == null || properties.Length <= 0)
                return;

            var oldDoc = Database.Documents.Get(key, transactionInformation);

            if (oldDoc == null)
            {
                return;
            }
            
            foreach (var property in metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints))
            {
                var constraint = Util.GetConstraint(property);

                var newProp = jsonReplicationDocument[constraint.PropName];

                // Handle Updates in the constraint since it changed
                var prefix = "UniqueConstraints/" + entityName + constraint.PropName + "/";
                
                var oldProp = oldDoc.DataAsJson[constraint.PropName];

                string[] oldUniqueValues;
                if (!Util.TryGetUniqueValues(oldProp, out oldUniqueValues))
                    continue;

                string[] newUniqueValues;
                if (Util.TryGetUniqueValues(newProp, out newUniqueValues))
                {
                    var join = (from oldValue in oldUniqueValues
                             join newValue in newUniqueValues
                                 on oldValue equals newValue
                             select oldValue);

                    if (join.Count() == oldUniqueValues.Count())
                        continue;        
                }
                
                foreach (var oldUniqueValue in oldUniqueValues)
                {
                    var escapedUniqueValue = Util.EscapeUniqueValue(oldUniqueValue, constraint.CaseInsensitive);
                    var uniqueConstraintsDocumentKey = prefix + escapedUniqueValue;
                    var uniqueConstraintsDocument = Database.Documents.Get(uniqueConstraintsDocumentKey, transactionInformation);

                    if (uniqueConstraintsDocument == null)
                        continue;

                    uniqueConstraintsDocument = DeepCloneDocument(uniqueConstraintsDocument);

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

        private static JsonDocument DeepCloneDocument(JsonDocument uniqueConstraintsDocument)
        {
            //This is a very expensive deep clone, i don't want to expose it as a method of JsonDocument.
            //The reason i do this is because Snapshoting is shallow and we need a deep snapshot.
            JsonDocument clone = new JsonDocument();
            clone.DataAsJson =(RavenJObject)(uniqueConstraintsDocument.DataAsJson.CloneToken());
            clone.Metadata = (RavenJObject)(uniqueConstraintsDocument.Metadata.CloneToken());
            return clone;
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

        private static void ConvertUniqueConstraintsDocumentIfNecessary(JsonDocument uniqueConstraintsDocument, string escapedUniqueValue)
        {
            var oldFormat = uniqueConstraintsDocument.DataAsJson.ContainsKey("RelatedId");
            if (oldFormat == false)
                return;         

            var key = uniqueConstraintsDocument.DataAsJson.Value<string>("RelatedId");
            uniqueConstraintsDocument.DataAsJson.Remove("RelatedId");

            AddConstraintToUniqueConstraintsDocument(uniqueConstraintsDocument, escapedUniqueValue, key);
        }

        private static bool RemoveConstraintFromUniqueConstraintDocument(JsonDocument uniqueConstraintsDocument, string escapedUniqueValue)
        {
            if (uniqueConstraintsDocument.DataAsJson.ContainsKey("RelatedId"))
                return uniqueConstraintsDocument.DataAsJson.Remove("RelatedId");

            var constraints = (RavenJObject)uniqueConstraintsDocument.DataAsJson["Constraints"];

            return constraints.Remove(escapedUniqueValue);
        }

        private static void AddConstraintToUniqueConstraintsDocument(JsonDocument uniqueConstraintsDocument, string escapedUniqueValue, string key)
        {
            if (!uniqueConstraintsDocument.DataAsJson.ContainsKey("Constraints"))
                uniqueConstraintsDocument.DataAsJson["Constraints"] = new RavenJObject();

            var constraints = (RavenJObject)uniqueConstraintsDocument.DataAsJson["Constraints"];

            constraints[escapedUniqueValue] = RavenJObject.FromObject(new { RelatedId = key });
        }

        private static string GetRelatedIdFromUniqueConstraintsDocument(JsonDocument uniqueConstraintsDocument, string escapedUniqueValue)
        {
            if (uniqueConstraintsDocument.DataAsJson.ContainsKey("RelatedId"))
                return uniqueConstraintsDocument.DataAsJson.Value<string>("RelatedId");

            var constraints = (RavenJObject)uniqueConstraintsDocument.DataAsJson["Constraints"];

            RavenJToken value;
            if (constraints.TryGetValue(escapedUniqueValue, out value))
                return value.Value<string>("RelatedId");

            return null;
        }

        public override IEnumerable<string> GeneratedMetadataNames
        {
            get
            {
                return new[]
                {
                    Constants.IsConstraintDocument,
                    Constants.EnsureUniqueConstraints
                };
            }
        }
    }
}
