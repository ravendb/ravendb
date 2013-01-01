using System;
using System.IO.IsolatedStorage;
using System.Threading.Tasks;
using System.Windows;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio
{
	public partial class App : Application
	{
		public App()
		{
			Exit += HandleExit;
			Startup += Application_Startup;
			UnhandledException += Application_UnhandledException;

			LoadDefaults();
			InitializeComponent();
		}

		private void Application_Startup(object sender, StartupEventArgs e)
		{
			SettingsRegister.Register();

			Schedulers.UIScheduler = TaskScheduler.FromCurrentSynchronizationContext();

			var rootVisual = new MainPage();
			ApplicationModel.Current.Setup(rootVisual);
			RootVisual = rootVisual;
		}

		private void LoadDefaults()
		{
			Settings.Instance.LoadSettings(IsolatedStorageSettings.ApplicationSettings);
		}

		private void HandleExit(object sender, EventArgs e)
		{
			foreach (var databaseChanges in ApplicationModel.ChangesToDispose)
			{
				var toDispose = databaseChanges as IDisposable;
				if(toDispose != null)
					toDispose.Dispose();
			}
			Settings.Instance.LastUrl = UrlUtil.Url;
			Settings.Instance.SaveSettings(IsolatedStorageSettings.ApplicationSettings);

			IsolatedStorageSettings.ApplicationSettings.Save();
		}

		private void Application_UnhandledException(object sender, ApplicationUnhandledExceptionEventArgs e)
		{
			if (System.Diagnostics.Debugger.IsAttached) 
                return;
			
			e.Handled = true;
			ApplicationModel.Current.AddErrorNotification(e.ExceptionObject, "An unhandled exception occurred: " + e.ExceptionObject.Message);
		}
	}
}