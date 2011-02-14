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
	public class BrowseDocumentsViewModel : Conductor<DocumentViewModel>.Collection.OneActive, IRavenScreen,
	                                        IHandle<DocumentDeleted>
	{
		readonly IEventAggregator events;
		readonly IServer server;
		readonly IWindowManager windowManager;
		bool isBusy;

		[ImportingConstructor]
		public BrowseDocumentsViewModel(IServer server, IWindowManager windowManager, IEventAggregator events)
		{
			DisplayName = "Documents";
			this.server = server;
			this.windowManager = windowManager;
			this.events = events;
			events.Subscribe(this);
		}

		public BindablePagedQuery<JsonDocument, DocumentViewModel> Documents { get; private set; }

		public bool IsBusy
		{
			get { return isBusy; }
			set
			{
				isBusy = value;
				NotifyOfPropertyChange(() => IsBusy);
			}
		}

		public void Handle(DocumentDeleted message)
		{
			var deleted = Documents.Where(x => x.Id == message.DocumentId).FirstOrDefault();

			if (deleted != null)
				Documents.Remove(deleted);
		}

		public SectionType Section
		{
			get { return SectionType.Documents; }
		}

		public void GetAll(IList<JsonDocument> response)
		{
			var vm = IoC.Get<DocumentViewModel>();
			var result = response
				.Select(vm.CloneUsing)
				.ToList();

			Items.AddRange(result);
			IsBusy = false;
		}

		protected override void OnActivate()
		{
			IsBusy = true;

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
			IsBusy = false;

			NotifyOfPropertyChange(() => Documents);
		}
	}
}