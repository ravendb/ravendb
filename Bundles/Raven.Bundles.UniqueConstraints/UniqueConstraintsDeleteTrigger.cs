using System.Collections.Generic;
using System.Linq;

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
				var propName = property.Value<string>(); // the name of the constraint property

				var prefix = "UniqueConstraints/" + entityName + property + "/"; // UniqueConstraints/EntityNamePropertyName/
				var prop = doc.DataAsJson[propName];
				var array = prop as RavenJArray;
				var checkKeys = array != null ? array.Select(p => p.Value<string>()) : new[] {prop.Value<string>()};

				foreach (var checkKey in checkKeys)
				{
					Database.Delete(prefix + checkKey, null, transactionInformation);
				}
			}
		}
	}
}