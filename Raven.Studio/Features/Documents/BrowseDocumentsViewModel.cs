namespace Raven.Studio.Features.Documents
{
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Messages;
	using Plugin;
	using Raven.Database;

	public class BrowseDocumentsViewModel : RavenScreen, IDatabaseScreenMenuItem,
		IHandle<DocumentDeleted>
	{
		readonly IEventAggregator events;
		readonly IServer server;

		public int Index { get { return 40; } }

		[ImportingConstructor]
		public BrowseDocumentsViewModel(IServer server, IEventAggregator events)
			: base(events)
		{
			DisplayName = "Documents";
			this.server = server;
			this.events = events;
			events.Subscribe(this);


			server.Connected += delegate
			{
				using (var session = server.OpenSession())
					Documents = new BindablePagedQuery<JsonDocument, DocumentViewModel>(
						session.Advanced.AsyncDatabaseCommands.GetDocumentsAsync, 
						jdoc => new DocumentViewModel(jdoc));

				Documents.PageSize = 25;
			};

		}

		public BindablePagedQuery<JsonDocument, DocumentViewModel> Documents { get; private set; }

		public void Handle(DocumentDeleted message)
		{
			if (Documents == null) return;

			var deleted = Documents.Where(x => x.Id == message.DocumentId).FirstOrDefault();

			if (deleted != null)
				Documents.Remove(deleted);
		}

		public void CreateNewDocument()
		{
			var doc = IoC.Get<EditDocumentViewModel>();
			events.Publish(new DatabaseScreenRequested(() => doc));
		}

		public bool HasDocuments { get { return Documents.Any(); } }

		protected override void OnActivate()
		{
			if(Documents == null) return;

			WorkStarted();

			using (var session = server.OpenSession())
			{
				Documents.Query = session.Advanced.AsyncDatabaseCommands.GetDocumentsAsync;

				session.Advanced.AsyncDatabaseCommands
					.GetStatisticsAsync()
					.ContinueOnSuccess(x => RefreshDocuments(x.Result.CountOfDocuments));
			}
		}

		public void RefreshDocuments(long total)
		{
			Documents.GetTotalResults = () => total;
			Documents.LoadPage(() =>
								{
									NotifyOfPropertyChange(() => HasDocuments);
									WorkCompleted();
								});
		}
	}
}