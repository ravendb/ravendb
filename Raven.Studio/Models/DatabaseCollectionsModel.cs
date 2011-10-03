using System.Linq;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class DatabaseCollectionsModel : Model
	{
		private readonly IAsyncDatabaseCommands databaseCommands;
		public BindableCollection<CollectionModel> Collections { get; set; }
		public Observable<CollectionModel> SelectedCollection { get; set; }

		public DatabaseCollectionsModel(IAsyncDatabaseCommands databaseCommands)
		{
			this.databaseCommands = databaseCommands;
			Collections = new BindableCollection<CollectionModel>(new PrimaryKeyComparer<CollectionModel>(model=>model.Name));
			SelectedCollection = new Observable<CollectionModel>();
			ForceTimerTicked();
		}

		protected override System.Threading.Tasks.Task TimerTickedAsync()
		{
			return databaseCommands.GetTermsCount("Raven/DocumentsByEntityName", "Tag", "", 100)
				.ContinueOnSuccess(Update);
		}

		private void Update(NameAndCount[] collections)
		{
			var collectionModels = collections.OrderByDescending(x => x.Count).Select(col => new CollectionModel(databaseCommands)
			{
				Name = col.Name,
				Count = col.Count
			}).ToArray();
			Collections.Match(collectionModels);

			if (SelectedCollection.Value == null)
				SelectedCollection.Value = collectionModels.FirstOrDefault();
		}
	}
}