using System.Windows.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Views
{
	public partial class Edit : View
	{
		private readonly ICommand saveCommand;
		private readonly ICommand refreshCommand;

		public Edit()
		{
			InitializeComponent();

			var model = (EditableDocumentModel)DataContext;
			saveCommand = model.Save;
			refreshCommand = model.Refresh;

			KeyDown +=OnKeyDown;
		}

		private void OnKeyDown(object sender, KeyEventArgs args)
		{
			switch (args.Key)
			{
				case Key.S:
					if (args.Key == Key.Ctrl)
						Command.ExecuteCommand(saveCommand);
					break;
				case Key.R:
					if (args.Key == Key.Ctrl)
						Command.ExecuteCommand(refreshCommand);
					break;
			}
		}
	}
}