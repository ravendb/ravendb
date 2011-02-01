namespace Raven.Studio
{
	using System.Diagnostics;
	using System.Windows;
	using Controls;

	public partial class App : Application
	{
		public App()
		{
			UnhandledException += OnUnhandledException;

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