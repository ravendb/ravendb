using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
	public class DatabaseModel : Model
	{
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		private string name;

		public Observable<TaskModel> SelectedTask { get; set; }

		public DatabaseModel(string name, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			Name = name;
			this.asyncDatabaseCommands = asyncDatabaseCommands;

			Tasks = new BindableCollection<TaskModel>(new PrimaryKeyComparer<TaskModel>(x => x))
			{
				new ImportTask(asyncDatabaseCommands),
				new ExportTask(asyncDatabaseCommands)
			};
			SelectedTask = new Observable<TaskModel> {Value = Tasks.FirstOrDefault()};
			Statistics = new Observable<DatabaseStatistics>();
		}

		public BindableCollection<TaskModel> Tasks { get; private set; }

		public BindableCollection<string> Indexes { get; private set; }


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

		protected override Task TimerTickedAsync()
		{
			return asyncDatabaseCommands.GetStatisticsAsync()
				.ContinueOnSuccess(stats => Statistics.Value = stats);
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