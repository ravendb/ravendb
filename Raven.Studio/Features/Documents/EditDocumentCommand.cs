using System;
using Newtonsoft.Json;
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
				var projection = JsonConvert.SerializeObject(viewableDocument.InnerDocument);
				ApplicationModel.Current.Navigate(new Uri("/Edit?projection=" + Uri.EscapeDataString(projection), UriKind.Relative));
			}
			else
			{
				ApplicationModel.Current.Navigate(new Uri("/Edit?id=" + viewableDocument.Id, UriKind.Relative));
			}
		}
	}
}