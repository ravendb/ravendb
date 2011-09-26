using System;
using System.Linq;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class DatabaseCollectionsModel : NotifyPropertyChangedBase
	{
		private readonly IAsyncDatabaseCommands databaseCommands;
		public BindableCollection<CollectionModel> Collections { get; set; }
		public Observable<CollectionModel> Selected { get; set; }

		public DatabaseCollectionsModel(IAsyncDatabaseCommands databaseCommands)
		{
			this.databaseCommands = databaseCommands;
			Collections = new BindableCollection<CollectionModel>();
			Selected = new Observable<CollectionModel>();
		}

		public void Update(NameAndCount[] collections)
		{
			Collections.Match(collections.OrderByDescending(x => x.Count).Select(col => new CollectionModel(databaseCommands)
			{
				Name = col.Name,
				Count = col.Count
			}).ToArray());

			if (Selected.Value == null)
				Selected.Value = Collections.FirstOrDefault();
		}
	}
}