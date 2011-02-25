namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Messages;
	using Plugin;
	using Shell;

	public class DeleteDocument
	{
		readonly IEventAggregator events;
		readonly IServer server;
		readonly ShowMessageBox showMessageBox;

		[ImportingConstructor]
		public DeleteDocument(IServer server, IEventAggregator events, ShowMessageBox showMessageBox)
		{
			this.server = server;
			this.events = events;
			this.showMessageBox = showMessageBox;
		}

		public void Execute(string documentId)
		{
			showMessageBox(
				"Are you sure that you want to do this document? (" + documentId + ")",
				"Confirm Deletion",
				MessageBoxOptions.OkCancel,
				box => { if (box.WasSelected(MessageBoxOptions.Ok)) ExecuteDeletion(documentId); });
		}

		public bool CanExecute(string documentId)
		{
			return !string.IsNullOrEmpty(documentId);
		}

		void ExecuteDeletion(string documentId)
		{
			using (var session = server.OpenSession())
			session.Advanced.AsyncDatabaseCommands.DeleteDocumentAsync(documentId);

			events.Publish(new StatisticsUpdateRequested());
			events.Publish(new DocumentDeleted(documentId));
		}
	}
}