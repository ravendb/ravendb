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
			this.Startup += this.Application_Startup;
			this.UnhandledException += this.Application_UnhandledException;

			InitializeComponent();
		}

		private void Application_Startup(object sender, StartupEventArgs e)
		{
			SettingsRegister.Register();

			var rootVisual = new MainPage();
			ApplicationModel.Current.Setup(rootVisual);
			this.RootVisual = rootVisual;
		}

		private void Application_UnhandledException(object sender, ApplicationUnhandledExceptionEventArgs e)
		{
			if (System.Diagnostics.Debugger.IsAttached) return;
			
			e.Handled = true;
			var ex = e.ExceptionObject;
			if (ErrorHandler.Handle(ex) == false)
				ErrorPresenter.Show(ex);
		}
	}
}