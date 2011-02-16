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
	public class BrowseDocumentsViewModel : Conductor<DocumentViewModel>.Collection.OneActive,
	                                        IHandle<DocumentDeleted>
	{
		readonly IEventAggregator events;
		readonly IServer server;

		[ImportingConstructor]
		public BrowseDocumentsViewModel(IServer server, IEventAggregator events)
		{
			DisplayName = "Documents";
			this.server = server;
			this.events = events;
			events.Subscribe(this);
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
			var doc = IoC.Get<DocumentViewModel>();
			events.Publish(new DatabaseScreenRequested(() => doc));	
		}

		public void GetAll(IList<JsonDocument> response)
		{
			var vm = IoC.Get<DocumentViewModel>();
			var result = response
				.Select(vm.CloneUsing)
				.ToList();

			Items.AddRange(result);
		}

		protected override void OnActivate()
		{
			using (var session = server.OpenSession())
			{
				var vm = IoC.Get<DocumentViewModel>();
				Documents = new BindablePagedQuery<JsonDocument, DocumentViewModel>(
					session.Advanced.AsyncDatabaseCommands.GetDocumentsAsync, vm.CloneUsing);
				Documents.PageSize = 25;

				session.Advanced.AsyncDatabaseCommands
					.GetStatisticsAsync()
					.ContinueOnSuccess(x => RefreshDocuments(x.Result.CountOfDocuments));
			}
		}

		public void RefreshDocuments(long total)
		{
			Documents.GetTotalResults = () => total;
			Documents.LoadPage();

			NotifyOfPropertyChange(() => Documents);
		}
	}
}