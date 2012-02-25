namespace Raven.Bundles.UniqueConstraints
{
	using Abstractions.Data;
	using Database.Plugins;
	using Json.Linq;

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
				var checkKey = "UniqueConstraints/" + entityName  + property + "/" + doc.DataAsJson.Value<string>(property.Value<string>());

				Database.Delete(checkKey, null, transactionInformation);
			}
		}
	}
}
