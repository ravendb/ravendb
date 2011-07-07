using Raven.Studio.Infrastructure.Navigation;

namespace Raven.Studio.Features.Tasks
{
	using Caliburn.Micro;
	using Database;
	using Framework;
	using Framework.Extensions;
	using Plugins;

	public abstract class ConsoleOutputTask : RavenScreen
	{
		string status;

		protected ConsoleOutputTask()
		{
			Console = new BindableCollection<string>();
			Server.CurrentDatabaseChanged += delegate { ClearConsole(); };
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

		protected override NavigationState GetScreenNavigationState()
		{
			return null;
		}
	}
}