using Raven.Client.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
	public class SaveDocumentCommand : Command
	{
		private readonly EditableDocument document;
		private readonly IAsyncDatabaseCommands databaseCommands;

		public SaveDocumentCommand(EditableDocument document)
		{
			this.document = document;
			this.databaseCommands = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands;
		}

		public override void Execute(object parameter)
		{
			document.Notify = "saving document...";
			databaseCommands.PutAsync(document.Key, document.Etag, RavenJObject.Parse(document.JsonData),
									  RavenJObject.Parse(document.JsonMetadata))
				.ContinueOnSuccess(result =>
				                   	{
				                   		document.Notify = result.Key + " document saved";
				                   	})
				.Catch(exception => document.Notify = null);
		}
	}
}