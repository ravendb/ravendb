using System.Linq;
using Raven.Studio.Features.Query;
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

		public override bool CanExecute(object parameter)
		{
			return viewableDocument != null;
		}

		public override void Execute(object parameter)
		{
			var urlParser = new UrlParser("/edit");

			if (string.IsNullOrEmpty(viewableDocument.Id))
			{
				var key = ProjectionData.Projections.First(x => x.Value == viewableDocument).Key;
				urlParser.SetQueryParam("projection", key);
			}
			else
			{
				urlParser.SetQueryParam("id", viewableDocument.Id);
			}

			UrlUtil.Navigate(urlParser.BuildUrl());
		}
	}
}