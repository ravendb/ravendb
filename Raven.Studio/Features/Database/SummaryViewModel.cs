namespace Raven.Studio.Features.Database
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Documents;
	using Framework;
	using Messages;
	using Plugin;

	[Export]
	public class SummaryViewModel : Conductor<IScreen>.Collection.OneActive, IHandle<DocumentDeleted>
	{
		readonly IServer server;
		readonly DocumentTemplateProvider templateProvider;

		[ImportingConstructor]
		public SummaryViewModel(IServer server, TemplateColorProvider colorProvider, IEventAggregator events)
		{
			this.server = server;
			this.templateProvider = new DocumentTemplateProvider(server, colorProvider);
			events.Subscribe(this);

			DisplayName = "Summary";
		}

		public string DatabaseName
		{
			get { return server.CurrentDatabase; }
		}

		public IServer Server
		{
			get { return server; }
		}

		public BindableCollection<EditDocumentViewModel> RecentDocuments { get; private set; }

		public IEnumerable<Collection> Collections { get; private set; }

		public long LargestCollectionCount
		{
			get
			{
				return (Collections == null || !Collections.Any())
				       	? 0
				       	: Collections.Max(x => x.Count);
			}
		}

		public SectionType Section
		{
			get { return SectionType.Documents; }
		}

		public void Handle(DocumentDeleted message)
		{
			RecentDocuments
				.Where(x => x.Id == message.DocumentId)
				.ToList()
				.Apply(x => RecentDocuments.Remove(x));

			//TODO: update collections
			//Collections
			//    .Where(x => x.Name == message.Document.CollectionType)
			//    .Apply(x => x.Count--);
		}

		protected override void OnActivate()
		{
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetCollectionsAsync(0, 25)
					.ContinueOnSuccess(x =>
					                   	{
					                   		Collections = x.Result;
					                   		NotifyOfPropertyChange(() => LargestCollectionCount);
					                   		NotifyOfPropertyChange(() => Collections);
					                   	});

				session.Advanced.AsyncDatabaseCommands
					.GetDocumentsAsync(0, 12)
					.ContinueOnSuccess(x =>
					                   	{
					                   		//TODO: I don't like this...
					                   		var vm = IoC.Get<EditDocumentViewModel>();
					                   		RecentDocuments = new BindableCollection<EditDocumentViewModel>(
					                   			x.Result.Select(vm.CloneUsing));
					                   		NotifyOfPropertyChange(() => RecentDocuments);
					                   	});
			}
		}
	}
}