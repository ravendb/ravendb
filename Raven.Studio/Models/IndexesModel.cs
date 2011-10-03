using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class IndexesModel : Model
	{
		private readonly IAsyncDatabaseCommands databaseCommands;
		public BindableCollection<string> Indexes { get; private set; }

		public IndexesModel(IAsyncDatabaseCommands databaseCommands)
		{
			this.databaseCommands = databaseCommands;
			Indexes = new BindableCollection<string>(new PrimaryKeyComparer<string>(name => name));
			ForceTimerTicked();
		}

		protected override Task TimerTickedAsync()
		{
			return databaseCommands
				.GetIndexNamesAsync(0, 256)
				.ContinueOnSuccess(Indexes.Match);
		}
	}
}