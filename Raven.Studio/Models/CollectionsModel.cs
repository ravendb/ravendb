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
	public class CollectionsModel : ViewModel
	{
		private static string initialSelectedDatabaseName;
		public static WeakReference<BindableCollection<CollectionModel>> Collections { get; set; }
		public static WeakReference<Observable<CollectionModel>> SelectedCollection { get; set; }
		public static WeakReference<Observable<DocumentsModel>> DocumentsForSelectedCollection { get; set; }

		static CollectionsModel()
		{
			Collections = new WeakReference<BindableCollection<CollectionModel>>(new BindableCollection<CollectionModel>(model => model.Name, new KeysComparer<CollectionModel>(model => model.Count)));
			SelectedCollection = new WeakReference<Observable<CollectionModel>>(new Observable<CollectionModel>());
			DocumentsForSelectedCollection = new WeakReference<Observable<DocumentsModel>>(new Observable<DocumentsModel> {Value = new DocumentsModel {CustomFetchingOfDocuments = GetFetchDocumentsMethod}});

			SelectedCollection.Target.PropertyChanged += (sender, args) =>
			                                      	{
			                                      		PutCollectionNameInTheUrl();
														DocumentsForSelectedCollection.Target.Value.ForceTimerTicked();
			                                      	};
		}

		private static Task GetFetchDocumentsMethod(DocumentsModel documentsModel)
		{
			if (SelectedCollection.Target.Value == null || string.IsNullOrWhiteSpace(SelectedCollection.Target.Value.Name))
				return Execute.EmptyResult<string>();

			var name = SelectedCollection.Target.Value.Name;
			return ApplicationModel.DatabaseCommands
				.QueryAsync("Raven/DocumentsByEntityName", new IndexQuery { Start = documentsModel.Pager.Skip, PageSize = documentsModel.Pager.PageSize, Query = "Tag:" + name }, new string[] { })
				.ContinueOnSuccess(queryResult =>
				{
					var documents = SerializationHelper.RavenJObjectsToJsonDocuments(queryResult.Results);
					documentsModel.Documents.Match(documents.Select(x => new ViewableDocument(x)).ToArray());
					DocumentsForSelectedCollection.Target.Value.Pager.TotalResults.Value = queryResult.TotalResults;
				})
				.CatchIgnore<InvalidOperationException>(() => ApplicationModel.Current.AddNotification(new Notification("Unable to retrieve collections from server.", NotificationLevel.Error)));
		}

		private static void PutCollectionNameInTheUrl()
		{
			var urlParser = new UrlParser(UrlUtil.Url);
			var collection = SelectedCollection.Target.Value;
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
			OnPropertyChanged();
			ModelUrl = "/collections";
			SelectedCollection.Target.PropertyChanged += (sender, args) => OnPropertyChanged("ViewTitle");
		}

		public override void LoadModelParameters(string parameters)
		{
			var urlParser = new UrlParser(parameters);
			var name = urlParser.GetQueryParam("name");
			if (DocumentsForSelectedCollection.Target.Value != null)
			{
				DocumentsForSelectedCollection.Target.Value.Pager.SetSkip(urlParser);
				ForceTimerTicked();
			}
			initialSelectedDatabaseName = name;
		}

		public override Task TimerTickedAsync()
		{
			return DatabaseCommands.GetTermsCount("Raven/DocumentsByEntityName", "Tag", "", 100)
				.ContinueOnSuccess(Update)
				.CatchIgnore<WebException>(() =>
				       	{
							var urlParser = new UrlParser(UrlUtil.Url);
				       		if (urlParser.RemoveQueryParam("name"))
				       			UrlUtil.Navigate(urlParser.BuildUrl());
				       		ApplicationModel.Current.AddNotification(new Notification("Unable to retrieve collections from server.", NotificationLevel.Error));
				       	});
		}

		private void Update(NameAndCount[] collections)
		{
			var collectionModels = collections.OrderByDescending(x => x.Count).Select(col => new CollectionModel
			{
				Name = col.Name,
				Count = col.Count
			}).ToArray();

			Collections.Target.Match(collectionModels, AfterUpdate);
		}

		private void AfterUpdate()
		{
			if (initialSelectedDatabaseName != null &&
				(SelectedCollection.Target.Value == null || SelectedCollection.Target.Value.Name != initialSelectedDatabaseName || Collections.Target.Contains(SelectedCollection.Target.Value) == false))
			{
				SelectedCollection.Target.Value = Collections.Target.FirstOrDefault(x => x.Name == initialSelectedDatabaseName);
			}

			if (SelectedCollection.Target.Value == null)
				SelectedCollection.Target.Value = Collections.Target.FirstOrDefault();
		}

		public string ViewTitle
		{
			get
			{
				if (SelectedCollection.Target.Value == null || string.IsNullOrEmpty(SelectedCollection.Target.Value.Name))
					return "Collections";
				return "Collection: " + SelectedCollection.Target.Value.Name;
			}
		}
	}
}