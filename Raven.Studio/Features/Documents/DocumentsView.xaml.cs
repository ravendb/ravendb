using System.Linq;
using System.Windows.Controls;
using System.Windows.Input;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
	public partial class DocumentsView : View
	{
		private readonly DeleteDocumentsCommand deleteDocuments = new DeleteDocumentsCommand();

		public DocumentsView()
		{
			InitializeComponent();

			DocumentsList.SelectionChanged += DocumentsListOnSelectionChanged;
			DocumentsList.KeyDown += DocumentsListOnKeyDown;
		}

		private void DocumentsListOnKeyDown(object sender, KeyEventArgs args)
		{
			switch (args.Key)
			{
				case Key.Delete:
					Command.ExecuteCommand(deleteDocuments, sender);
					break;
			}
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