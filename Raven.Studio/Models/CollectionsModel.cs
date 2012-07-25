using System;
using System.Linq;
using System.Net;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Notification = Raven.Studio.Messages.Notification;
using Raven.Studio.Extensions;

namespace Raven.Studio.Models
{
	public class CollectionsModel : PageViewModel, IHasPageTitle
	{
	    private const string CollectionsIndex = "Raven/DocumentsByEntityName";
	    private string initialSelectedDatabaseName;
		public BindableCollection<CollectionModel> Collections { get; set; }
		public Observable<CollectionModel> SelectedCollection { get; set; }

	    private CollectionDocumentsCollectionSource collectionSource; 
		private DocumentsModel documentsForSelectedCollection;

        public CollectionDocumentsCollectionSource CollectionSource
		{
			get
			{
                if (collectionSource == null)
                    collectionSource = new CollectionDocumentsCollectionSource { CollectionName  = GetSelectedCollectionName()};
                return collectionSource;
			}
		}

	    private string GetSelectedCollectionName()
	    {
	        return SelectedCollection.Value != null ? SelectedCollection.Value.Name  : "";
	    }

	    public DocumentsModel DocumentsForSelectedCollection
	    {
	        get
	        {
	            if (documentsForSelectedCollection == null)
                    documentsForSelectedCollection = new DocumentsModel(CollectionSource);
	            return documentsForSelectedCollection;
	        }
	    }

		private void PutCollectionNameInTheUrl()
		{
			var urlParser = new UrlParser(UrlUtil.Url);
			var collection = SelectedCollection.Value;
			if (collection == null)
				return;
			var name = collection.Name;
			initialSelectedDatabaseName = name;
			if (urlParser.GetQueryParam("name") != name)
			{
				urlParser.SetQueryParam("name", name);
				UrlUtil.Navigate(urlParser.BuildUrl());
			}
		}

		public CollectionsModel()
		{
			ModelUrl = "/collections";

            Collections = new BindableCollection<CollectionModel>(model => model.Name);
            SelectedCollection = new Observable<CollectionModel>();

            DocumentsForSelectedCollection.SetChangesObservable(d =>  d.IndexChanges
                                 .Where(n =>n.Name.Equals(CollectionsIndex,StringComparison.InvariantCulture))
                                 .Select(m => Unit.Default));

		    DocumentsForSelectedCollection.DocumentNavigatorFactory =
		        (id, index) =>
		        DocumentNavigator.Create(id, index, CollectionsIndex,
		                                 new IndexQuery() {Query = "Tag:" + GetSelectedCollectionName()});

            SelectedCollection.PropertyChanged += (sender, args) =>
            {
                PutCollectionNameInTheUrl();
                CollectionSource.CollectionName = GetSelectedCollectionName();

                DocumentsForSelectedCollection.Context = "Collection/" + GetSelectedCollectionName();
            };
		}

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);
			var name = urlParser.GetQueryParam("name");
			initialSelectedDatabaseName = name;
		}

		private void RefreshCollectionsList()
		{
			DatabaseCommands.GetTermsCount(CollectionsIndex, "Tag", "", 100)
				.ContinueOnSuccess(collections =>
				                   	{
										var collectionModels = collections.OrderByDescending(x => x.Count)
											.Where(x=>x.Count > 0)
											.Select(col => new CollectionModel { Name = col.Name, Count = col.Count })
											.ToArray();

				                   		Collections.Match(collectionModels, () => AfterUpdate(collections));
				                   	})
				.Catch(ex =>
				                           	{
				                           		var urlParser = new UrlParser(UrlUtil.Url);
				                           		if (urlParser.RemoveQueryParam("name"))
				                           			UrlUtil.Navigate(urlParser.BuildUrl());
				                           		ApplicationModel.Current.AddErrorNotification(ex, "Unable to retrieve collections from server.");
				                           	});
		}

		private void AfterUpdate(NameAndCount[] collectionDocumentsCount)
		{
            // update documents count
		    var nameToCount = collectionDocumentsCount.ToDictionary(i => i.Name, i => i.Count);
		    foreach (var collectionModel in Collections)
		    {
		        collectionModel.Count = nameToCount[collectionModel.Name];
		    }

			if (initialSelectedDatabaseName != null &&
				(SelectedCollection.Value == null || SelectedCollection.Value.Name != initialSelectedDatabaseName || Collections.Contains(SelectedCollection.Value) == false))
			{
				SelectedCollection.Value = Collections.FirstOrDefault(x => x.Name == initialSelectedDatabaseName);
			}

			if (SelectedCollection.Value == null)
				SelectedCollection.Value = Collections.FirstOrDefault();
		}

		public string PageTitle
		{
			get
			{
				if (SelectedCollection.Value == null || string.IsNullOrEmpty(SelectedCollection.Value.Name))
					return "Collections";
				return "Collection: " + SelectedCollection.Value.Name;
			}
		}

        protected override void OnViewLoaded()
        {
            var databaseChanged = Database.ObservePropertyChanged()
                .Select(_ => Unit.Default)
                .TakeUntil(Unloaded);

            databaseChanged
                .Do(_ => RefreshCollectionsList())
                .Subscribe(_ => ObserveIndexChanges(databaseChanged));

            ObserveIndexChanges(databaseChanged);
            RefreshCollectionsList();
        }

	    private void ObserveIndexChanges(IObservable<Unit> databaseChanged)
	    {
            if (Database.Value != null)
            {
                Database.Value.IndexChanges
                    .Where(n => n.Name.Equals(CollectionsIndex, StringComparison.InvariantCulture))
                    .SampleResponsive(TimeSpan.FromSeconds(2))
                    .TakeUntil(Unloaded.Merge(databaseChanged))
                    .ObserveOnDispatcher()
                    .Subscribe(__ => RefreshCollectionsList());
            }
	    }
	}
}