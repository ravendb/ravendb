namespace Raven.Bundles.UniqueConstraints
{
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Abstractions.Commands;
	using Raven.Abstractions.Data;
	using Raven.Database.Plugins;
	using Raven.Json.Linq;

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

			var entityName = metadata.Value<string>(Constants.RavenEntityName);

			if (string.IsNullOrEmpty(entityName))
			{
				return;
			}

			foreach (var property in metadata.Value<RavenJArray>(Constants.EnsureUniqueConstraints))
			{
				var checkKey = "UniqueConstraints/" + entityName + "/" + property + "/" + doc.DataAsJson.Value<string>(property.Value<string>());

				Database.Delete(checkKey, null, transactionInformation);
			}
		}
	}
}
