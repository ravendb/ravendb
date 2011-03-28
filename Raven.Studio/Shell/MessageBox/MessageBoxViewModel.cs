namespace Raven.Studio.Shell.MessageBox
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;

    [Export(typeof (IMessageBox))]
	[PartCreationPolicy(CreationPolicy.NonShared)]
	public class MessageBoxViewModel : Screen, IMessageBox
	{
		MessageBoxOptions selection;

		public bool OkVisible
		{
			get { return IsVisible(MessageBoxOptions.Ok); }
		}

		public bool CancelVisible
		{
			get { return IsVisible(MessageBoxOptions.Cancel); }
		}

		public bool YesVisible
		{
			get { return IsVisible(MessageBoxOptions.Yes); }
		}

		public bool NoVisible
		{
			get { return IsVisible(MessageBoxOptions.No); }
		}

		public string Message { get; set; }
		public MessageBoxOptions Options { get; set; }

		public void Ok()
		{
			selection = MessageBoxOptions.Ok;
			TryClose();
		}

		public void Cancel()
		{
			selection = MessageBoxOptions.Cancel;
			TryClose();
		}

		public void Yes()
		{
			selection = MessageBoxOptions.Yes;
			TryClose();
		}

		public void No()
		{
			selection = MessageBoxOptions.No;
			TryClose();
		}

		public bool WasSelected(MessageBoxOptions option)
		{
			return (selection & option) == option;
		}

		bool IsVisible(MessageBoxOptions option)
		{
			return (Options & option) == option;
		}
	}
}