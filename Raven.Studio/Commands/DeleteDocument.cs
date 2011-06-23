using System.Collections.Generic;

namespace Raven.Studio.Commands
{
    using System.Collections;
    using System.ComponentModel.Composition;
    using System.Linq;
    using Caliburn.Micro;
	using Features.Database;
    using Features.Documents;
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

		public bool CanExecute(IList<string> documentsIds)
		{
			return documentsIds != null && documentsIds.Count > 0 &&  string.IsNullOrWhiteSpace(documentsIds.First());
		}

		public void Execute(IList<string> documentsIds)
		{
			string message = documentsIds.Count > 1 ? string.Format("Are you sure you want to delete these {0} documents?", documentsIds.Count) : 
				string.Format("Are you sure that you want to do this document? ({0})", documentsIds.First());

			showMessageBox(
				message,
				"Confirm Deletion",
				MessageBoxOptions.OkCancel,
				box => {
					if (box.WasSelected(MessageBoxOptions.Ok))
					{
						documentsIds.Apply(ExecuteDeletion); // Is this the most efficient way?
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