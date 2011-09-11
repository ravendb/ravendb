using System;
using System.Windows;
using System.Windows.Input;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
	public class EditDocumentCommand : ICommand
	{
		private readonly ViewableDocument viewableDocument;

		public EditDocumentCommand(ViewableDocument viewableDocument)
		{
			this.viewableDocument = viewableDocument;
		}

		public bool CanExecute(object parameter)
		{
			return true;
		}

		public void Execute(object parameter)
		{
			ApplicationModel.Current.Navigate(new Uri("/Edit?id="+viewableDocument.Id, UriKind.Relative));
		}

		public event EventHandler CanExecuteChanged = delegate { };
	}
}