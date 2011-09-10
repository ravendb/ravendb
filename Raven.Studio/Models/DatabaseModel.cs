using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class DatabaseModel : NotifyPropertyChangedBase
	{
		private string name;
		public string Name
		{
			get { return name; }
			set { name = value; OnPropertyChanged(); }
		}

		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;

		public DatabaseModel(string name, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			Name = name;
			this.asyncDatabaseCommands = asyncDatabaseCommands;

			Statistics = new Observable<DatabaseStatistics>();

			//RecentDocuments = new CollectionRenewal<JsonDocument>(new JsonDocument[0],
			//                                                      () => this.asyncDatabaseCommands.GetDocumentsAsync(GetStart(), 15)
			//                                                                    .ContinueWith(x => (IEnumerable<JsonDocument>) x.Result));
			}

		
		public Observable<DatabaseStatistics> Statistics { get; set; }

		public BindableCollection<JsonDocument> RecentDocuments { get; set; }

		public bool Equals(DatabaseModel other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.name, name);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof (DatabaseModel)) return false;
			return Equals((DatabaseModel) obj);
		}

		public override int GetHashCode()
		{
			return (name != null ? name.GetHashCode() : 0);
		}
	}
}
