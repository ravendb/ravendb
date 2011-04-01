namespace Raven.Studio.Features.Tasks
{
	using Caliburn.Micro;
	using Database;

	public abstract class ConsoleOutputTask : Screen
	{
		protected readonly IServer server;

		protected ConsoleOutputTask(IServer server)
		{
			this.server = server;
			Console = new BindableCollection<string>();
			server.CurrentDatabaseChanged += delegate { ClearConsole(); };
		}
		public IObservableCollection<string> Console { get; private set; }

		public void ClearConsole()
		{
			Console.Clear();
		}
	}
}