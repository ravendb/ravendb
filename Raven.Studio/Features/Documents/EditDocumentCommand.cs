using System;
using System.Windows;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
	public class EditDocumentCommand : Command
	{
		private readonly ViewableDocument viewableDocument;

		public EditDocumentCommand(ViewableDocument viewableDocument)
		{
			this.viewableDocument = viewableDocument;
		}

		public override void Execute(object parameter)
		{
			if (string.IsNullOrEmpty(viewableDocument.Id))
			{
				ApplicationModel.Current.State = viewableDocument.InnerDocument;
				ApplicationModel.Current.Navigate(new Uri("/Edit?projection=true", UriKind.Relative));
			}
			else
			{
				ApplicationModel.Current.Navigate(new Uri("/Edit?id=" + viewableDocument.Id, UriKind.Relative));
			}
		}
	}
}