using System.Windows.Input;
using Raven.Studio.Features.Util;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Views
{
	public partial class Edit : View
	{
		private readonly ICommand saveCommand;
		private readonly ICommand refreshCommand;
		private bool isCtrlHold;

		public Edit()
		{
			InitializeComponent();

			var model = ((Observable<EditableDocumentModel>)DataContext).Value;
			saveCommand = model.Save;
			refreshCommand = model.Refresh;

			KeyDown += OnKeyDown;
			KeyUp += OnKeyUp;
		}

		private void OnKeyUp(object sender, KeyEventArgs args)
		{
			switch (args.Key)
			{
				case Key.Ctrl:
					isCtrlHold = false;
					break;
			}
		}

		private void OnKeyDown(object sender, KeyEventArgs args)
		{
			switch (args.Key)
			{
				case Key.S:
					if (isCtrlHold)
						Command.ExecuteCommand(saveCommand);
					break;
				case Key.R:
					if (isCtrlHold)
						Command.ExecuteCommand(refreshCommand);
					break;
				case Key.F:
					if (isCtrlHold)
					{
						SearchTool.IsActive = true;
						//var searchWindow = new SearchWindow();
						//searchWindow.Show();

						//searchWindow.Closed += (s, e) =>
						//{
						//    if (searchWindow.DialogResult == true)
						//    {
						//        string result = searchWindow.Result.ToLower();


						//        var item = document.DataContext as EditableDocumentModel;
						//        if (item != null)
						//        {
						//            var data = item.JsonData.ToLower();

						//            var index = data.IndexOf(result, System.StringComparison.Ordinal);
						//        }
						//    }
					}
					break;
				case Key.Escape:
					if (SearchTool.IsActive)
						SearchTool.IsActive = false;
					break;
				case Key.Ctrl:
					isCtrlHold = true;
					break;
			}
		}
	}
}