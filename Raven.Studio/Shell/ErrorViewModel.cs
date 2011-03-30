namespace Raven.Studio.Shell
{
	using Caliburn.Micro;

	public class ErrorViewModel : Screen
	{
		public static bool ErrorAlreadyShown { get; private set; }

		public ErrorViewModel()
		{
			DisplayName = "Error!";
		}

		public string Message { get; set; }
		public string Details { get; set; }

		protected override void OnDeactivate(bool close)
		{
			ErrorAlreadyShown = false;
		}

		protected override void OnActivate()
		{
			ErrorAlreadyShown = true;
		}
	}
}