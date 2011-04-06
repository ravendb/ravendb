namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Database;
	using Features.Documents;
	using Framework.Extensions;
	using Messages;
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
			events.Publish(new WorkStarted());
			server.OpenSession().Advanced.AsyncDatabaseCommands
				.GetAsync(documentId)
				.ContinueOnSuccess( get =>
				               	{
                                    if(get.Result == null)
                                    {
                                        var msg = string.Format("No document with id {0} was found.", documentId);
                                        showMessageBox(msg, "Document Not found");
                                        return;
                                    }

				               		var doc = IoC.Get<EditDocumentViewModel>();
									doc.Initialize(get.Result);
									events.Publish(new DatabaseScreenRequested(() => doc));
									events.Publish(new WorkCompleted());
				               	});
		}
	}
}