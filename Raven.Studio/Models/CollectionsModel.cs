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
	public class CollectionsModel : ViewModel, IHasPageTitle
	{
		private static string initialSelectedDatabaseName;
		public static BindableCollection<CollectionModel> Collections { get; set; }
		public static Observable<CollectionModel> SelectedCollection { get; set; }

		private static WeakReference<Observable<DocumentsModel>> documentsForSelectedCollection;
		public static Observable<DocumentsModel> DocumentsForSelectedCollection
		{
			get
			{
				if (documentsForSelectedCollection == null || documentsForSelectedCollection.IsAlive == false)
					documentsForSelectedCollection = new WeakReference<Observable<DocumentsModel>>(new Observable<DocumentsModel> {Value = new DocumentsModel {CustomFetchingOfDocuments = GetFetchDocumentsMethod}});
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
				DocumentsForSelectedCollection.Value.ForceTimerTicked();
			};
		}

		private static Task GetFetchDocumentsMethod(DocumentsModel documentsModel)
		{
			string name;
			if (SelectedCollection.Value == null || string.IsNullOrWhiteSpace(name = SelectedCollection.Value.Name))
				return Execute.EmptyResult<string>();

			return ApplicationModel.DatabaseCommands
				.QueryAsync("Raven/DocumentsByEntityName", new IndexQuery {Start = documentsModel.Pager.Skip, PageSize = documentsModel.Pager.PageSize, Query = "Tag:" + name}, new string[] {})
				.ContinueOnSuccess(queryResult =>
				                   	{
				                   		var documents = SerializationHelper.RavenJObjectsToJsonDocuments(queryResult.Results)
				                   			.Select(x => new ViewableDocument(x))
				                   			.ToArray();
				                   		documentsModel.Documents.Match(documents);
										if (DocumentsForSelectedCollection.Value.Pager.TotalResults.Value.HasValue == false || DocumentsForSelectedCollection.Value.Pager.TotalResults.Value.Value != queryResult.TotalResults)
										{
											DocumentsForSelectedCollection.Value.Pager.TotalResults.Value = queryResult.TotalResults;
										}
				                   	})
				.CatchIgnore<InvalidOperationException>(() => ApplicationModel.Current.AddNotification(new Notification("Unable to retrieve collections from server.", NotificationLevel.Error)));
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
			if (DocumentsForSelectedCollection.Value != null)
			{
				DocumentsForSelectedCollection.Value.Pager.SetSkip(urlParser);
				ForceTimerTicked();
			}
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