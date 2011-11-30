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
					ExecuteCommand(deleteDocuments, sender);
					break;
			}
		}

		private void ExecuteCommand(ICommand command, object param = null)
		{
			if (command.CanExecute(param))
				command.Execute(param);
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