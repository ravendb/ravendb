namespace Raven.Studio.Features.Tasks
{
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Framework.Extensions;

	public abstract class ConsoleOutputTask : RavenScreen
	{
		protected readonly IServer server;
		string status;

		protected ConsoleOutputTask(IServer server, IEventAggregator events)
			: base(events)
		{
			this.server = server;
			Console = new BindableCollection<string>();
			server.CurrentDatabaseChanged += delegate { ClearConsole(); };
		}

		public IObservableCollection<string> Console { get; private set; }

		public string Status
		{
			get { return status; }
			set
			{
				status = value;
				NotifyOfPropertyChange(() => Status);
			}
		}

		public void ClearConsole()
		{
			Console.Clear();
			Status = string.Empty;
		}

		protected void Output(string format, params object[] args)
		{
			Console.Add(format, args);
		}
	}
}