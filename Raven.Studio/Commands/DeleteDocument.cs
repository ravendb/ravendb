using System.Collections.Generic;
using Raven.Studio.Features.Collections;

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

		public bool CanExecute(IList<DocumentViewModel> documents)
		{
			if (documents == null || documents.Count == 0)
				return false;

			var document = documents.First();
			return document != null && document.CollectionType != BuiltinCollectionName.Projection;
		}

		public void Execute(IList<DocumentViewModel> documents)
		{
			string message = documents.Count > 1 ? string.Format("Are you sure you want to delete these {0} documents?", documents.Count) :
				string.Format("Are you sure that you want to do this document? ({0})", documents.First().Id);

			showMessageBox(
				message,
				"Confirm Deletion",
				MessageBoxOptions.OkCancel,
				box => {
					if (box.WasSelected(MessageBoxOptions.Ok))
					{
						documents.Apply(document => ExecuteDeletion(document.Id)); // Is this the most efficient way?
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