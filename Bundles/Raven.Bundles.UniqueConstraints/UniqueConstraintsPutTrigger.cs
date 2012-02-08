namespace Raven.Bundles.UniqueConstraints
{
	using System.Collections.Generic;
	using System.Linq;
	using System.Text;

	using Raven.Abstractions.Data;
	using Raven.Database.Data;
	using Raven.Database.Plugins;
	using Raven.Json.Linq;

	public class UniqueConstraintsPutTrigger : AbstractPutTrigger
	{
		public override void AfterPut(string key, RavenJObject document, RavenJObject metadata, System.Guid etag, TransactionInformation transactionInformation)
		{
			if (key.StartsWith("Raven"))
			{
				return;
			}

			var entityName = metadata.Value<string>(Constants.RavenEntityName);

			if (string.IsNullOrEmpty(entityName))
			{
				return;
			}

			var properties = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

			if (properties != null && properties.Count() > 0)
			{
				var constraintMetaObject = new RavenJObject { { Constants.EnsureUniqueConstraints, true } };
				foreach (var property in metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints))
				{
					var propName = ((RavenJValue)property).Value.ToString();
					var putResult = Database.Put(
						"UniqueConstraints/" + entityName + "/" + propName + "/" + document.Value<string>(propName),
						null,
						RavenJObject.FromObject(new { Id = key }),
						constraintMetaObject,
						transactionInformation);
				}
			}
		}

		public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key.StartsWith("Raven"))
			{
				return VetoResult.Allowed;
			}

			var entityName = metadata.Value<string>(Constants.RavenEntityName);

			if (string.IsNullOrEmpty(entityName))
			{
				return VetoResult.Allowed;
			}

			var properties = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

			if (properties != null && properties.Count() > 0)
			{
				var invalidFields = new StringBuilder();

				foreach (var property in properties)
				{
					var propName = ((RavenJValue)property).Value.ToString();
					var checkKey = "UniqueConstraints/" + entityName + "/" + propName + "/" + document.Value<string>(propName);
					var checkDoc = Database.Get(checkKey, transactionInformation);
					if (checkDoc != null)
					{
						var checkId = checkDoc.ToJson().Value<string>("Id");

						if (checkId != key)
						{
							invalidFields.Append(property + ", ");
						}
					}
				}

				if (invalidFields.Length > 0)
				{
					invalidFields.Length = invalidFields.Length - 2;
					return VetoResult.Deny("Ensure unique constraint violated for fields: " + invalidFields);
				}
			}

			return VetoResult.Allowed;
		}

		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (key.StartsWith("Raven"))
			{
				return;
			}

			var entityName = metadata.Value<string>(Constants.RavenEntityName);

			if (string.IsNullOrEmpty(entityName))
			{
				return;
			}

			var properties = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

			if (properties != null && properties.Count() > 0)
			{
				var oldDoc = Database.Get(key, transactionInformation);

				if (oldDoc == null)
				{
					return;
				}

				var oldJson = oldDoc.DataAsJson;

				foreach (var property in metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints))
				{
					var propName = ((RavenJValue)property).Value.ToString();

					// Handle Updates in the Constraint
					if (!oldJson.Value<string>(propName).Equals(document.Value<string>(propName)))
					{
						Database.Delete("UniqueConstraints/" + entityName + "/" + propName + "/" + oldJson.Value<string>(propName), null, transactionInformation);
					}
				}
			}
			
		}
	}
}
