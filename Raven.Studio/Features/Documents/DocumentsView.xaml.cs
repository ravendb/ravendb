using System.Linq;
using System.Windows.Controls;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
	public partial class DocumentsView : View
	{
		public DocumentsView()
		{
			InitializeComponent();

			DocumentsList.SelectionChanged += DocumentsListOnSelectionChanged;
		}

		private void DocumentsListOnSelectionChanged(object sender, SelectionChangedEventArgs e)
		{
			var commands = DocumentsContextMenu.Items
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