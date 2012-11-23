namespace Raven.Bundles.UniqueConstraints
{
	using Abstractions.Data;
	using Database.Plugins;
	using Json.Linq;
    using System.Collections.Generic;
    using System.Linq;
	using Raven.Database.Extensions;
	using System;

	public class UniqueConstraintsDeleteTrigger : AbstractDeleteTrigger
	{
		public override void OnDelete(string key, TransactionInformation transactionInformation)
		{
			if (key.StartsWith("Raven"))
				return;

			var doc = Database.Get(key, transactionInformation);

			if (doc == null)
				return;

			var metadata = doc.Metadata;

			var entityName = metadata.Value<string>(Constants.RavenEntityName) + "/";

			var uniqueConstraints = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

			if (uniqueConstraints == null)
				return;

            foreach (var property in uniqueConstraints) // each unique constraint property
			{
				var checkKey = "UniqueConstraints/" + entityName  + property + "/" + doc.DataAsJson.Value<string>(property.Value<string>());
			    var propName = property.Value<string>(); // the name of the constraint property
			    IEnumerable<string> checkKeys;

			    var prefix = "UniqueConstraints/" + entityName + property + "/"; // UniqueConstraints/EntityNamePropertyName/
			    var prop = doc.DataAsJson[propName];
			    if (prop is RavenJArray)
			        checkKeys = ((RavenJArray) prop).Select(p => p.Value<string>());
			    else
			        checkKeys = new[] {prop.Value<string>()};
				var value = property.Value<string>();
				if(value == null)
					continue;
				var checkKey = "UniqueConstraints/" + entityName + property + "/" +
							   Util.EscapeUniqueValue(doc.DataAsJson.Value<string>(value));

			    foreach(var checkKey in checkKeys)
                    Database.Delete(prefix+checkKey, null, transactionInformation);
			}
		}
	}
}
