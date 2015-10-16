using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Impl.Streams
{
	public class DatabaseSmugglerStreamDocumentActions : DatabaseSmugglerStreamActionsBase, IDatabaseSmugglerDocumentActions
	{
		private readonly JsonTextWriter _writer;

		public DatabaseSmugglerStreamDocumentActions(JsonTextWriter writer)
			: base(writer, "Docs")
		{
			_writer = writer;
		}

		public Task WriteDocumentAsync(RavenJObject document)
		{
			document.WriteTo(_writer);
			return new CompletedTask();
		}
	}
}