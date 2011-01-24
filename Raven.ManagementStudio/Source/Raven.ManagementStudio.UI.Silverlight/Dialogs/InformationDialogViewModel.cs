namespace Raven.ManagementStudio.UI.Silverlight.Dialogs
{
    using Caliburn.Micro;

    public class InformationDialogViewModel : Screen
    {
        public InformationDialogViewModel(string displayName, string message)
        {
            this.DisplayName = displayName;
            this.Message = message;
        }

        public string Message { get; set; }
    }
}
