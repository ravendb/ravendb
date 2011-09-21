using System;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
	public class SaveDocumentCommand : Command
	{
		private readonly EditableDocumentModel document;
		private readonly IAsyncDatabaseCommands databaseCommands;

		public SaveDocumentCommand(EditableDocumentModel document)
		{
			this.document = document;
			this.databaseCommands = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands;
		}

		public override void Execute(object parameter)
		{
			if (document.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
			{
				AskUser.ConfirmationAsync("Confirm Edit", "Are you sure that you want to edit a system document?")
					.ContinueWhenTrue(SaveDocument);
				return;
			}

			SaveDocument();
		}

		private void SaveDocument()
		{
			document.Notice.Value = "saving document...";
			databaseCommands.PutAsync(document.Key, document.Etag,
									  RavenJObject.Parse(document.JsonData),
									  RavenJObject.Parse(document.JsonMetadata))
				.ContinueOnSuccess(result =>
								   {
									   document.Notice.Value = result.Key + " document saved";
									   document.Etag = result.ETag;
								   })
				.Catch(exception => document.Notice.Value = null);
		}
	}
}