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
		    this.Exit += HandleExit;
			this.Startup += this.Application_Startup;
			this.UnhandledException += this.Application_UnhandledException;

			InitializeComponent();
		}

	    private void Application_Startup(object sender, StartupEventArgs e)
		{
			SettingsRegister.Register();

	        Schedulers.UIScheduler = TaskScheduler.FromCurrentSynchronizationContext();

			var rootVisual = new MainPage();
			ApplicationModel.Current.Setup(rootVisual);
			this.RootVisual = rootVisual;

		    LoadDefaults();
		}

	    private void LoadDefaults()
	    {
	        DocumentSize.Current.LoadDefaults(IsolatedStorageSettings.ApplicationSettings);
	    }

	    private void HandleExit(object sender, EventArgs e)
	    {
            DocumentSize.Current.SaveDefaults(IsolatedStorageSettings.ApplicationSettings);

            IsolatedStorageSettings.ApplicationSettings.Save();
	    }

	    private void Application_UnhandledException(object sender, ApplicationUnhandledExceptionEventArgs e)
		{
			if (System.Diagnostics.Debugger.IsAttached) return;
			
			e.Handled = true;
			ApplicationModel.Current.AddErrorNotification(e.ExceptionObject, "An unhandled exception occurred: " + e.ExceptionObject.Message);
		}
	}
}
