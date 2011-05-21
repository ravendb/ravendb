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

        public bool CanExecute(object listOrId)
        {
            if (listOrId == null)
                return false;

            var list = listOrId as IList;
            if (list != null)
            {
                return list.Count > 0;
            }

            var viewModel = listOrId as string;
            return !string.IsNullOrWhiteSpace(viewModel);
        }

        public void Execute(object listOrId)
		{
            var list = listOrId as IList;
            string message;
            
            if(list != null) {
                if(list.Count > 1)
                    message = string.Format("Are you sure you want to delete these {0} documents?", list.Count);
                else message = "Are you sure that you want to do this document? (" + ((DocumentViewModel)list[0]).Id + ")";
            }
            else message = "Are you sure that you want to do this document? (" + listOrId + ")";


			showMessageBox(
				message,
				"Confirm Deletion",
				MessageBoxOptions.OkCancel,
				box => {
				    if (box.WasSelected(MessageBoxOptions.Ok)) {
				        if(list != null) {
				            list.OfType<DocumentViewModel>().Apply(x => ExecuteDeletion(x.Id)); //Is this the most efficient way?
				        }
                        else {
				            ExecuteDeletion(listOrId.ToString());
				        }
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