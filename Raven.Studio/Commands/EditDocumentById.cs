namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Documents;
	using Framework.Extensions;
	using Messages;
	using Plugins;
	using Shell.MessageBox;

    public class EditDocumentById
	{
		readonly IEventAggregator events;
		readonly IServer server;
        readonly ShowMessageBox showMessageBox;

        [ImportingConstructor]
		public EditDocumentById(IEventAggregator events, IServer server, ShowMessageBox showMessageBox)
		{
			this.events = events;
			this.server = server;
		    this.showMessageBox = showMessageBox;
		}

		public void Execute(string documentId)
		{
			events.Publish(new WorkStarted("searching for document by id"));

			server.OpenSession().Advanced.AsyncDatabaseCommands
				.GetAsync(documentId)
				.ContinueOnSuccess( get =>
				               	{

									events.Publish(new WorkCompleted("searching for document by id"));

                                    if(get.Result == null)
                                    {
										var msg = string.Format("Could not locate a document with id {0}.", documentId);
                                    	events.Publish(new NotificationRaised(msg));
                                    	showMessageBox(msg, "Document Not found");
                                        return;
                                    }

				               		var doc = IoC.Get<EditDocumentViewModel>();
									doc.Initialize(get.Result);
									events.Publish(new DatabaseScreenRequested(() => doc));
				               	});
		}
	}
}