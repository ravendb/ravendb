using System.Collections.ObjectModel;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexDefinitionModel : Model
	{
		private IAsyncDatabaseCommands asyncDatabaseCommands;
		private IndexDefinition index;
		private string name;

		public IndexDefinitionModel(IndexDefinition index, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			this.asyncDatabaseCommands = asyncDatabaseCommands;
			UpdateFromDocument(index);
		}

		private void UpdateFromDocument(IndexDefinition indexDefinition)
		{
			this.index = indexDefinition;
			this.name = index.Name;

						
			OnEverythingChanged();
		}
	}
}