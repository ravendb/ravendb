using System.Collections.Generic;

namespace Raven.Bundles.UniqueConstraints
{
	using System.Linq;
	using System.Text;

	using Abstractions.Data;
	using Database.Plugins;
	using Json.Linq;

	public class UniqueConstraintsPutTrigger : AbstractPutTrigger
	{
		public override void AfterPut(string key, RavenJObject document, RavenJObject metadata, System.Guid etag, TransactionInformation transactionInformation)
		{
			if (key.StartsWith("Raven/"))
			{
				return;
			}

			var entityName = metadata.Value<string>(Constants.RavenEntityName) + "/";

			var properties = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

			if (properties == null || !properties.Any()) 
				return;

			var constraintMetaObject = new RavenJObject { { Constants.IsConstraintDocument, true } };
			foreach (var property in properties)
			{
				var propName = ((RavenJValue)property).Value.ToString();
                var prop = document[propName];

			    IEnumerable<string> relatedKeys;
                var prefix = "UniqueConstraints/" + entityName + property + "/";

			    if (prop is RavenJArray)
			        relatedKeys = ((RavenJArray) prop).Select(p => p.Value<string>());
			    else
			        relatedKeys = new[] {prop.Value<string>()};

                foreach (var relatedKey in relatedKeys)
                {
                    Database.Put(
                        prefix + relatedKey,
                        null,
                        RavenJObject.FromObject(new {RelatedId = key}),
                        constraintMetaObject,
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
                var propName = property.Value<string>();
                IEnumerable<string> checkKeys;

                var prefix = "UniqueConstraints/" + entityName + property + "/";
                var prop = document[propName];
                if (prop is RavenJArray)
                    checkKeys = ((RavenJArray)prop).Select(p => p.Value<string>());
                else
                    checkKeys = new[] { prop.Value<string>() };

                foreach (var checkKey in checkKeys)
                {
                    var checkDoc = Database.Get(prefix + checkKey, transactionInformation);
                    if (checkDoc == null)
                        continue;

                    var checkId = checkDoc.DataAsJson.Value<string>("RelatedId");

                    if (checkId != key)
                        invalidFields.Append(property + ", ");
                    break;
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
		        return;

		    var entityName = metadata.Value<string>(Constants.RavenEntityName) +"/";

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
				var propName = ((RavenJValue)property).Value.ToString();
                IEnumerable<string> deleteKeys;

			    if (oldJson.Value<string>(propName).Equals(document.Value<string>(propName))) continue;

                // Handle Updates in the constraint since it changed
			    var prefix = "UniqueConstraints/" + entityName + property + "/";
			    var prop = document[propName];
			    if (prop is RavenJArray)
			        deleteKeys = ((RavenJArray)prop).Select(p => p.Value<string>());
			    else
			        deleteKeys = new[] { prop.Value<string>() };

			    foreach (var deleteKey in deleteKeys)
			        Database.Delete(prefix + deleteKey, null, transactionInformation);
			}
		}
	}
}
