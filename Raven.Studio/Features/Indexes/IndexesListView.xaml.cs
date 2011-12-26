using System.Linq;
using System.Windows.Controls;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Indexes
{
	public partial class IndexesListView : UserControl
	{
		public IndexesListView()
		{
			InitializeComponent();

			IndexesList.SelectionChanged += DocumentsListOnSelectionChanged;
		}
	
		private void DocumentsListOnSelectionChanged(object sender, SelectionChangedEventArgs e)
		{
			var commands = IndexesContextMenu.Items
				.Cast<MenuItem>()
				.Select(item => item.Command)
				.OfType<Command>();
			
			foreach (var command in commands)
			{
				command.RaiseCanExecuteChanged();
			}
		}
	}
}