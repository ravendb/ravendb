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
		private static string initialSelectedDatabaseName;
		public static BindableCollection<CollectionModel> Collections { get; set; }
		public static Observable<CollectionModel> SelectedCollection { get; set; }

	    private static WeakReference<CollectionDocumentsCollectionSource> collectionSource; 
		private static WeakReference<DocumentsModelEnhanced> documentsForSelectedCollection;

        public static CollectionDocumentsCollectionSource CollectionSource
		{
			get
			{
                if (collectionSource == null || collectionSource.IsAlive == false)
                    collectionSource = new WeakReference<CollectionDocumentsCollectionSource>(new CollectionDocumentsCollectionSource { CollectionName  = GetSelectedCollectionName()});
                return collectionSource.Target;
			}
		}

	    private static string GetSelectedCollectionName()
	    {
	        return SelectedCollection.Value != null ? SelectedCollection.Value.Name  : "";
	    }

	    public static DocumentsModelEnhanced DocumentsForSelectedCollection
	    {
	        get
	        {
	            if (documentsForSelectedCollection == null || documentsForSelectedCollection.IsAlive == false)
                    documentsForSelectedCollection = new WeakReference<DocumentsModelEnhanced>(new DocumentsModelEnhanced(CollectionSource));
	            return documentsForSelectedCollection.Target;
	        }
	    }

		static CollectionsModel()
		{
			Collections = new BindableCollection<CollectionModel>(model => model.Name, new KeysComparer<CollectionModel>(model => model.Count));
			SelectedCollection = new Observable<CollectionModel>();

			SelectedCollection.PropertyChanged += (sender, args) =>
			{
				PutCollectionNameInTheUrl();
				CollectionSource.CollectionName = GetSelectedCollectionName();
			};
		}

		private static void PutCollectionNameInTheUrl()
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
				urlParser.RemoveQueryParam("skip");
				UrlUtil.Navigate(urlParser.BuildUrl());
			}
		}

		public CollectionsModel()
		{
			ModelUrl = "/collections";
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