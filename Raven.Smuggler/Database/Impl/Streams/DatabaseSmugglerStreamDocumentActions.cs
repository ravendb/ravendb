using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Impl.Streams
{
	public class DatabaseSmugglerStreamDocumentActions : DatabaseSmugglerStreamActionsBase, IDatabaseSmugglerDocumentActions
	{
		public DatabaseSmugglerStreamDocumentActions(JsonTextWriter writer)
			: base(writer, "Docs")
		{
		}

		public Task WriteDocumentAsync(RavenJObject document)
		{
			document.WriteTo(Writer);
			return new CompletedTask();
		}
	}
}