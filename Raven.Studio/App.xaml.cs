namespace Raven.Studio
{
	using System.Diagnostics;
	using System.Windows;
	using Controls;

	public partial class App : Application
	{
		// ReSharper disable UnaccessedField.Local
		readonly AppBootstrapper _bootstrapper;
		// ReSharper restore UnaccessedField.Local

		public App()
		{
			UnhandledException += OnUnhandledException;

			_bootstrapper = new AppBootstrapper();

			InitializeComponent();
		}

		static void OnUnhandledException(object sender, ApplicationUnhandledExceptionEventArgs e)
		{
			if (!Debugger.IsAttached)
			{
				e.Handled = true;
				var errorWin = new ErrorWindow(e.ExceptionObject);
				errorWin.Show();
			}
		}
	}
}