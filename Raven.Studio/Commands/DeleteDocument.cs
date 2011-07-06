using System.Collections.Generic;

namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
    using System.Linq;
    using Caliburn.Micro;
	using Messages;
	using Plugins;
	using Shell.MessageBox;

	[Export]
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

		public bool CanExecute(IList<string> documentIds)
		{
			if (documentIds == null || documentIds.Count == 0)
				return false;

			return string.IsNullOrEmpty(documentIds.First()) == false;
		}

		public void Execute(IList<string> documentIds)
		{
			string message = documentIds.Count > 1 ? string.Format("Are you sure you want to delete these {0} documents?", documentIds.Count) :
				string.Format("Are you sure that you want to delete this document? ({0})", documentIds.First());

			showMessageBox(
				message,
				"Confirm Deletion",
				MessageBoxOptions.OkCancel,
				box => {
					if (box.WasSelected(MessageBoxOptions.Ok))
					{
						documentIds.Apply(ExecuteDeletion); // Is this the most efficient way?
					}
				});
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