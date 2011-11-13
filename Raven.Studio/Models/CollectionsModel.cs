using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class CollectionsModel : ViewModel
	{
		private string initialSelectedDatabaseName;
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
			if (string.IsNullOrEmpty(null) == false)
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
			Collections.Match(collectionModels);

			if (initialSelectedDatabaseName != null)
			{
				SelectedCollection.Value = collectionModels.Where(x => x.Name == initialSelectedDatabaseName).FirstOrDefault();
				initialSelectedDatabaseName = null;
			}
			if (SelectedCollection.Value == null)
				SelectedCollection.Value = collectionModels.FirstOrDefault();
		}
	}
}