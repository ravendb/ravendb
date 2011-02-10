namespace Raven.Studio.Dialogs
{
	using Caliburn.Micro;

	public class InformationDialogViewModel : Screen
	{
		public InformationDialogViewModel(string displayName, string message)
		{
			DisplayName = displayName;
			Message = message;
		}

		public string Message { get; set; }
	}
}