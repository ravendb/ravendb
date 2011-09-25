using System;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
	public class DeleteDocumentCommand : Command
	{
		private readonly string key;
		private readonly IAsyncDatabaseCommands databaseCommands;

		public DeleteDocumentCommand(string key, IAsyncDatabaseCommands databaseCommands)
		{
			this.key = key;
			this.databaseCommands = databaseCommands;
		}

		public override void Execute(object parameter)
		{
			AskUser.ConfirmationAsync("Confirm Delete", "Really delete " + key + " ?")
				.ContinueWhenTrue(DeleteDocument);
		}

		private void DeleteDocument()
		{
			databaseCommands.DeleteDocumentAsync(key)
				.ContinueOnSuccess(() =>
				                   {
									   ApplicationModel.AddNotification(this.GetType(), key + " was successfully deleted.");
									   // ApplicationModel.Current.Navigate(new Uri("/Home", UriKind.Relative));
				                   })
								   .Catch();
		}
	}
}