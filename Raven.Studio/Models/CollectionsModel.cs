using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Models
{
	public class CollectionsModel : PageViewModel, IHasPageTitle
	{
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

            Collections = new BindableCollection<CollectionModel>(model => model.Name, new KeysComparer<CollectionModel>(model => model.Count));
            SelectedCollection = new Observable<CollectionModel>();

            SelectedCollection.PropertyChanged += (sender, args) =>
            {
                PutCollectionNameInTheUrl();
                CollectionSource.CollectionName = GetSelectedCollectionName();
                DocumentsForSelectedCollection.DocumentNavigatorFactory =
                    (id, index) =>
                    DocumentNavigator.Create(id, index, "Raven/DocumentsByEntityName",
                                             new IndexQuery() { Query = "Tag:" + GetSelectedCollectionName() });
                DocumentsForSelectedCollection.Context = "Collection/" + GetSelectedCollectionName();
            };
		}

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);
			var name = urlParser.GetQueryParam("name");
			initialSelectedDatabaseName = name;
		}

		public override Task TimerTickedAsync()
		{
			return DatabaseCommands.GetTermsCount("Raven/DocumentsByEntityName", "Tag", "", 100)
				.ContinueOnSuccess(collections =>
				                   	{
										var collectionModels = collections.OrderByDescending(x => x.Count)
											.Where(x=>x.Count > 0)
											.Select(col => new CollectionModel { Name = col.Name, Count = col.Count })
											.ToArray();

				                   		Collections.Match(collectionModels, AfterUpdate);
				                   	})
				.CatchIgnore<WebException>(() =>
				                           	{
				                           		var urlParser = new UrlParser(UrlUtil.Url);
				                           		if (urlParser.RemoveQueryParam("name"))
				                           			UrlUtil.Navigate(urlParser.BuildUrl());
				                           		ApplicationModel.Current.AddNotification(new Notification("Unable to retrieve collections from server.", NotificationLevel.Error));
				                           	});
		}

		private void AfterUpdate()
		{
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
	}
}