using System;
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
			ApplicationModel.Current.Navigate(new Uri("/Edit?id="+viewableDocument.Id, UriKind.Relative));
		}
	}
}