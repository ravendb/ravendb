namespace Raven.Studio.Documents
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Net;
	using Caliburn.Micro;
	using Client;
	using Database;
	using Dialogs;
	using Framework;
	using Messages;
	using Newtonsoft.Json.Linq;
	using Plugin;
	using Raven.Database;
	using Shell;

	[Export]
	public class BrowseDocumentsViewModel : Conductor<DocumentViewModel>.Collection.OneActive, IRavenScreen,
		IHandle<DocumentDeleted>
	{
		bool isBusy;
		readonly IServer server;
		readonly IWindowManager windowManager;
		readonly IEventAggregator events;

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

			using(var session = server.OpenSession())
			{
				var vm = IoC.Get<DocumentViewModel>();
				Documents = new BindablePagedQuery<JsonDocument,DocumentViewModel>(
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

            NotifyOfPropertyChange( () => Documents);
		}

		public void Handle(DocumentDeleted message)
		{
			var deleted = Documents.Where(x=>x.Id == message.DocumentId).FirstOrDefault();

			if(deleted !=null)
				Documents.Remove(deleted);
		}
	}
}