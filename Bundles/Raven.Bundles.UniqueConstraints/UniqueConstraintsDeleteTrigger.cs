namespace Raven.Bundles.UniqueConstraints
{
	using Abstractions.Data;
	using Database.Plugins;
	using Json.Linq;
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

			var uniqueConstraits = metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints);

			if (uniqueConstraits == null)
				return;

			foreach (var property in uniqueConstraits)
			{
				var value = property.Value<string>();
				if(value == null)
					continue;
				var checkKey = "UniqueConstraints/" + entityName + property + "/" +
							   Util.EscapeUniqueValue(doc.DataAsJson.Value<string>(value));

				Database.Delete(checkKey, null, transactionInformation);
			}
		}
	}
}
