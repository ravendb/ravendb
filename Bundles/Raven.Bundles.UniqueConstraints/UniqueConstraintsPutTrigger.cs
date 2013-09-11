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
				if (prop == null || prop.Type == JTokenType.Null)
					continue;

                var prefix = "UniqueConstraints/" + entityName + constraint.PropName + "/";

				var array = prop as RavenJArray;
				var relatedKeys = array != null ? array.Select(p => p.Value<string>()) : new[] {prop.Value<string>()};

				foreach (var relatedKey in relatedKeys)
				{
					Database.Put(
                        prefix + Util.EscapeUniqueValue(relatedKey, constraint.CaseInsensitive),
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

				if (prop == null || prop.Type == JTokenType.Null)
					continue;

				var array = prop as RavenJArray;
				var checkKeys = array != null ? array.Select(p => p.Value<string>()) : new[] { prop.Value<string>() };

				foreach (var checkKey in checkKeys)
				{
                    var checkDoc = Database.Get(prefix + Util.EscapeUniqueValue(checkKey, constraint.CaseInsensitive), transactionInformation);

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

			var oldJson = oldDoc.DataAsJson;

			foreach (var property in metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints))
			{
			    var constraint = Util.GetConstraint(property);

                var oldValue = oldJson.Value<string>(constraint.PropName);
                if (oldValue == null || oldValue.Equals(document.Value<string>(constraint.PropName))) continue;

                // Handle Updates in the constraint since it changed
			    var prefix = "UniqueConstraints/" + entityName + constraint.PropName + "/";
                var prop = oldDoc.DataAsJson[constraint.PropName];
				if (prop == null || prop.Type == JTokenType.Null)
					continue;
				var array = prop as RavenJArray;

				var deleteKeys = array != null ? array.Select(p => p.Value<string>()) : new[] { prop.Value<string>() };

			    foreach (var deleteKey in deleteKeys)
			    {
                    Database.Delete(prefix + Util.EscapeUniqueValue(deleteKey, constraint.CaseInsensitive), null, transactionInformation);
			    }

			}
		}
	}
}