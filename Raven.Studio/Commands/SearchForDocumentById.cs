namespace Raven.Studio.Commands
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Database;
    using Framework;
    using Messages;
    using Plugin;

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
            using (var session = server.OpenSession())
            {
                session.Advanced.AsyncDatabaseCommands
                    .GetAsync(key)
                    .ContinueWith(get =>
                    {
                        if (get.Result == null)
                        {
                            events.Publish(new Notification("Could not locate a document with id " + key ));
                        }
                        else
                        {
                            var doc = IoC.Get<Database.DocumentViewModel>();
                            doc.Initialize(get.Result);
                            events.Publish(new OpenDocumentForEdit(doc));
                        }
                    });

            }
        }
    }
}