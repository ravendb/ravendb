using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Impl.Streams
{
	public class DatabaseSmugglerStreamIndexActions : DatabaseSmugglerStreamActionsBase, IDatabaseSmugglerIndexActions
	{
		private readonly JsonTextWriter _writer;

		public DatabaseSmugglerStreamIndexActions(JsonTextWriter writer)
			: base(writer, "Indexes")
		{
			_writer = writer;
		}

		public Task WriteIndexAsync(IndexDefinition index)
		{
			var indexAsJson = RavenJObject.FromObject(index);
			indexAsJson.WriteTo(_writer);
			return new CompletedTask();
		}
	}
}