using System.Linq;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class DatabaseCollectionsModel : NotifyPropertyChangedBase
	{
		private readonly IAsyncDatabaseCommands databaseCommands;
		public BindableCollection<CollectionModel> Collections { get; set; }
		public Observable<CollectionModel> SelectedCollection { get; set; }

		public DatabaseCollectionsModel(IAsyncDatabaseCommands databaseCommands)
		{
			this.databaseCommands = databaseCommands;
			Collections = new BindableCollection<CollectionModel>(new PrimaryKeyComparer<CollectionModel>(model=>model.Name));
			SelectedCollection = new Observable<CollectionModel>();
		}

		public void Update(NameAndCount[] collections)
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