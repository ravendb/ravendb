namespace Raven.Studio.Shell
{
	using Caliburn.Micro;

	public class ErrorViewModel : Screen
	{
		public ErrorViewModel()
		{
			DisplayName = "Error!";
		}

		public string Message { get; set; }
		public string Details { get; set; }
	}
}