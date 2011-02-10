namespace Raven.Studio.Collections
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Plugin;

	[Export]
	public class CollectionsViewModel : Screen
	{
		readonly IServer server;
		Collection activeCollection;

		[ImportingConstructor]
		public CollectionsViewModel(IServer server)
		{
			DisplayName = "Collections";

			this.server = server;
		}

		public IEnumerable<Collection> Collections { get; private set; }
		public IEnumerable<DocumentViewModel> ActiveCollectionDocuments { get; private set; }

		public Collection ActiveCollection
		{
			get { return activeCollection; }
			set
			{
				activeCollection = value;
				NotifyOfPropertyChange(() => ActiveCollection);
				GetDocumentsForActiveCollection();
			}
		}

		public long LargestCollectionCount
		{
			get
			{
				return (Collections == null || !Collections.Any())
				       	? 0
				       	: Collections.Max(x => x.Count);
			}
		}

		void GetDocumentsForActiveCollection()
		{
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetDocumentsAsync(0, 12)
					.ContinueOnSuccess(x =>
					                   	{
					                   		var templateProvider = IoC.Get<DocumentTemplateProvider>();
					                   		ActiveCollectionDocuments = new BindableCollection<DocumentViewModel>(
					                   			x.Result.Select(doc => new DocumentViewModel(doc, templateProvider)));
					                   		NotifyOfPropertyChange(() => ActiveCollectionDocuments);
					                   	});
			}
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

					                   		ActiveCollection = Collections.FirstOrDefault();
					                   	});
			}
		}
	}
}