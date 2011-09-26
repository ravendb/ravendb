using System;
using System.Windows;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
	public class DeleteDocumentCommand : Command
	{
		private readonly string key;
		private readonly IAsyncDatabaseCommands databaseCommands;
		private readonly bool navigateToHome;

		public DeleteDocumentCommand(string key, IAsyncDatabaseCommands databaseCommands, bool navigateToHome)
		{
			this.key = key;
			this.databaseCommands = databaseCommands;
			this.navigateToHome = navigateToHome;
		}

		public override void Execute(object parameter)
		{
			AskUser.ConfirmationAsync("Confirm Delete", "Really delete " + key + " ?")
				.ContinueWhenTrue(DeleteDocument);
		}

		private void DeleteDocument()
		{
			databaseCommands.DeleteDocumentAsync(key)
				.ContinueOnSuccess(() => ApplicationModel.Current.AddNotification(new Notification(string.Format("Document {0} was deleted", key))))
				.ContinueOnSuccess(() =>
								   {
									   if (navigateToHome)
										   ApplicationModel.Current.Navigate(new Uri("/Home", UriKind.Relative));
								   })
								   .Catch();
		}
	}
}