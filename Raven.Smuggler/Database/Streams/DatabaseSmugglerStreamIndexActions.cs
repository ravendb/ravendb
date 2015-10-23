using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Streams
{
	public class DatabaseSmugglerStreamIndexActions : DatabaseSmugglerStreamActionsBase, IDatabaseSmugglerIndexActions
	{
		public DatabaseSmugglerStreamIndexActions(JsonTextWriter writer)
			: base(writer, "Indexes")
		{
		}

		public Task WriteIndexAsync(IndexDefinition index, CancellationToken cancellationToken)
		{
			var indexAsJson = new RavenJObject
			{
				{ "name", index.Name },
				{ "definition", RavenJObject.FromObject(index) }
			};
			indexAsJson.WriteTo(Writer);
			return new CompletedTask();
		}
	}
}