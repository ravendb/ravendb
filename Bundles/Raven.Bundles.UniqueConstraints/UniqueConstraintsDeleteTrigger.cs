using System.Collections.Generic;
using System.Linq;
using Raven.Imports.Newtonsoft.Json.Linq;

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
			    var constraint = Util.GetConstraint(property);
                var prefix = "UniqueConstraints/" + entityName + constraint.PropName+ "/"; // UniqueConstraints/EntityNamePropertyName/
                var prop = doc.DataAsJson[constraint.PropName];

			    string[] uniqueValues;
                if (!Util.TryGetUniqueValues(prop, out uniqueValues))
                    continue;

				foreach (var uniqueValue in uniqueValues)
				{
					Database.Delete(prefix + Util.EscapeUniqueValue(uniqueValue, constraint.CaseInsensitive), null, transactionInformation);
				}
			}
		}
	}
}