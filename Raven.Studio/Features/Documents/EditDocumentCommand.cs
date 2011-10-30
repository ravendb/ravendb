using System;
using Newtonsoft.Json;
using Raven.Studio.Infrastructure;

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
				UrlUtil.Navigate("/edit?projection=" + Uri.EscapeDataString(projection));
			}
			else
			{
				UrlUtil.Navigate("/edit?id=" + viewableDocument.Id);
			}
		}
	}
}