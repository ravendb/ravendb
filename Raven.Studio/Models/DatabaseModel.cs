using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
	public class DatabaseModel : Model
	{
		public const string SystemDatabaseName = "System";

		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		private readonly string name;

		public Observable<TaskModel> SelectedTask { get; set; }

		public DatabaseModel(string name, DocumentStore documentStore)
		{
			this.name = name;

			Tasks = new BindableCollection<TaskModel>(x => x.Name)
			{
				new ImportTask(),
				new ExportTask(),
				new StartBackupTask(),
			};
			SelectedTask = new Observable<TaskModel> {Value = Tasks.FirstOrDefault()};
			Statistics = new Observable<DatabaseStatistics>();

			asyncDatabaseCommands = name.Equals(SystemDatabaseName, StringComparison.OrdinalIgnoreCase)
			                             	? documentStore.AsyncDatabaseCommands.ForDefaultDatabase()
			                             	: documentStore.AsyncDatabaseCommands.ForDatabase(name);
		}

		public BindableCollection<TaskModel> Tasks { get; private set; }

		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { return asyncDatabaseCommands; }
		}

		public string Name
		{
			get { return name; }
		}

		public Observable<DatabaseStatistics> Statistics { get; set; }

		public override Task TimerTickedAsync()
		{
			return asyncDatabaseCommands
				.GetStatisticsAsync()
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