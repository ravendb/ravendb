namespace Raven.Studio.Commands
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Features.Database;
    using Features.Documents;
    using Framework;
    using Framework.Extensions;
    using Messages;

	public class SearchForDocumentById
    {
        readonly IEventAggregator events;
        readonly IServer server;

        [ImportingConstructor]
        public SearchForDocumentById(IEventAggregator events, IServer server)
        {
            this.events = events;
            this.server = server;
        }

        public void Execute(string key)
        {
			events.Publish(new WorkStarted());

            using (var session = server.OpenSession())
            {
                session.Advanced.AsyncDatabaseCommands
                    .GetAsync(key)
                    .ContinueOnSuccess(get =>
                    {
                        if (get.Result == null)
                        {
                            events.Publish(new NotificationRaised("Could not locate a document with id " + key ));
                        }
                        else
                        {
                            var doc = IoC.Get<EditDocumentViewModel>();
                            doc.Initialize(get.Result);
                            events.Publish(new DatabaseScreenRequested(() => doc));
							events.Publish(new WorkCompleted());
                        }
                    });

            }
        }
    }
}