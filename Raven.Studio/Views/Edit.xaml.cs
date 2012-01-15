using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Primitives;
using Microsoft.Expression.Interactivity.Core;
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
			TabControl.SelectionChanged += TabChanged;

			SearchTool.SearchOptions = new EditorSearchOptions();
			SearchTool.SyntaxEditor = (SyntaxEditor)TabControl.SelectedContent;
		}

		private void TabChanged(object sender, SelectionChangedEventArgs e)
		{
			SearchTool.SyntaxEditor = (SyntaxEditor)TabControl.SelectedContent;			
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
						EnableSearch();
					}
					break;
				case Key.Escape:
					if (SearchTool.Visibility == Visibility.Visible)
					{
						DisableSearch();
					}
					break;
				case Key.Ctrl:
					isCtrlHold = true;
					break;
			}
		}

		private void DisableSearch()
		{
			SearchTool.Visibility = Visibility.Collapsed;
			SearchTool.IsEnabled = false;
		}

		private void EnableSearch()
		{
			SearchTool.Visibility = Visibility.Visible;
			SearchTool.IsEnabled = true;
		}

		private void Search_Click(object sender, RoutedEventArgs e)
		{
			if(SearchTool.Visibility == Visibility.Visible)
				DisableSearch();
			else
			{
				EnableSearch();
			}
		}
	}
}