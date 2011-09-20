using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
	public class DatabaseModel : Model
	{
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		private string name;

		public DatabaseModel(string name, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			Name = name;
			this.asyncDatabaseCommands = asyncDatabaseCommands;

			Statistics = new Observable<DatabaseStatistics>();
			RecentDocuments = new BindableCollection<ViewableDocument>();
		}


		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { return asyncDatabaseCommands; }
		}

		public string Name
		{
			get { return name; }
			set
			{
				name = value;
				OnPropertyChanged();
			}
		}

		
		public Observable<DatabaseStatistics> Statistics { get; set; }

		public BindableCollection<ViewableDocument> RecentDocuments { get; set; }

		protected override Task TimerTickedAsync()
		{
			return asyncDatabaseCommands.GetStatisticsAsync()
				.ContinueOnSuccess(stats => Statistics.Value = stats)
				.ContinueWith(task => asyncDatabaseCommands.GetDocumentsAsync(0, 15)
				                      	.ContinueOnSuccess(docs => RecentDocuments.Match(docs.Select(x=>new ViewableDocument(x)).ToArray())))
				.Unwrap();
		}

		private bool Equals(DatabaseModel other)
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