using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class CollectionsModel : ViewModel
	{
		private static string initialSelectedDatabaseName;
		public static BindableCollection<CollectionModel> Collections { get; set; }
		public static Observable<CollectionModel> SelectedCollection { get; set; }

		static CollectionsModel()
		{
			Collections = new BindableCollection<CollectionModel>(model => model.Name, new KeysComparer<CollectionModel>(model => model.Count));
			SelectedCollection = new Observable<CollectionModel>();

			SelectedCollection.PropertyChanged += (sender, args) =>
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
			                                      	};
		}

		public CollectionsModel()
		{
			ModelUrl = "/collections";
		}

		public override void LoadModelParameters(string parameters)
		{
			var name = new UrlParser(parameters).GetQueryParam("name");
			initialSelectedDatabaseName = name;
		}

		protected override Task TimerTickedAsync()
		{
			return DatabaseCommands.GetTermsCount("Raven/DocumentsByEntityName", "Tag", "", 100)
				.ContinueOnSuccess(Update);
		}

		private void Update(NameAndCount[] collections)
		{
			var collectionModels = collections.OrderByDescending(x => x.Count).Select(col => new CollectionModel(DatabaseCommands)
			{
				Name = col.Name,
				Count = col.Count
			}).ToArray();

			Collections.Match(collectionModels, AfterUpdate);
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
	}
}