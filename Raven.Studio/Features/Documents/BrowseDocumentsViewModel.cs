namespace Raven.Studio.Features.Documents
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Messages;
	using Plugin;
	using Raven.Database;

	[Export]
	[PartCreationPolicy(CreationPolicy.Shared)]
	public class BrowseDocumentsViewModel : RavenScreen,
		IHandle<DocumentDeleted>
	{
		readonly IEventAggregator events;
		readonly IServer server;

		[ImportingConstructor]
		public BrowseDocumentsViewModel(IServer server, IEventAggregator events)
			: base(events)
		{
			DisplayName = "Documents";
			this.server = server;
			this.events = events;
			events.Subscribe(this);

			var vm = IoC.Get<Database.DocumentViewModel>();

			server.Connected += delegate
			{
				using (var session = server.OpenSession())
					Documents = new BindablePagedQuery<JsonDocument, Database.DocumentViewModel>(
						session.Advanced.AsyncDatabaseCommands.GetDocumentsAsync, vm.CloneUsing);
				Documents.PageSize = 25;
			};

		}

		public BindablePagedQuery<JsonDocument, Database.DocumentViewModel> Documents { get; private set; }

		public void Handle(DocumentDeleted message)
		{
			if (Documents == null) return;

			var deleted = Documents.Where(x => x.Id == message.DocumentId).FirstOrDefault();

			if (deleted != null)
				Documents.Remove(deleted);
		}

		public void CreateNewDocument()
		{
			var doc = IoC.Get<Database.DocumentViewModel>();
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