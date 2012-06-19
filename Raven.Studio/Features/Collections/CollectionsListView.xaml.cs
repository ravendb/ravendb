using System.Linq;
using System.Windows.Controls;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Collections
{
	public partial class CollectionsListView : PageView
	{
		public CollectionsListView()
		{
			InitializeComponent();

			CollectionsList.SelectionChanged += CollectionsListOnSelectionChanged;
		}

		private void CollectionsListOnSelectionChanged(object sender, SelectionChangedEventArgs e)
		{
			var commands = CollectionsContextMenu.Items
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