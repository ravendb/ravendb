using System;
using System.Linq;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace Raven.Bundles.UniqueConstraints
{
	using System.Text;

	using Abstractions.Data;
	using Database.Plugins;
	using Json.Linq;

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
					Database.Put(
                        prefix + Util.EscapeUniqueValue(uniqueValue, constraint.CaseInsensitive),
						null,
						RavenJObject.FromObject(new {RelatedId = key}),
						new RavenJObject { { Constants.IsConstraintDocument, true } },
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
                    var checkDoc = Database.Get(prefix + Util.EscapeUniqueValue(uniqueValue, constraint.CaseInsensitive), transactionInformation);

					if (checkDoc == null)
						continue;

					var checkId = checkDoc.DataAsJson.Value<string>("RelatedId");

					if (checkId != key)
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

		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key.StartsWith("Raven/"))
			{
				return;
			}

			var entityName = metadata.Value<string>(Constants.RavenEntityName) + "/";

			var properties = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

			if (properties == null || properties.Length <= 0)
				return;

			var oldDoc = Database.Get(key, transactionInformation);

			if (oldDoc == null)
			{
				return;
			}
            
			foreach (var property in metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints))
			{
			    var constraint = Util.GetConstraint(property);

                var newProp = document[constraint.PropName];

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
                    Database.Delete(prefix + Util.EscapeUniqueValue(oldUniqueValue, constraint.CaseInsensitive), null, transactionInformation);
			    }

			}
		}
	}
}