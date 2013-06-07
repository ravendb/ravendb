using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
	public class EditNeighboringDocumentCommand : Command
	{
		public override bool CanExecute(object parameter)
		{
			return (parameter is FriendlyDocument);
		}

		public override void Execute(object parameter)
		{
			var urlParser = new UrlParser("/edit");
			var friendly = (parameter as FriendlyDocument);
			if (friendly != null)
			{
				urlParser.SetQueryParam(friendly.IsProjection ? "projection" : "id", friendly.Id);
				
				if (friendly.NeighborsIds != null)
					urlParser.SetQueryParam("neighbors", string.Join(",", friendly.NeighborsIds));
			}

			UrlUtil.Navigate(urlParser.BuildUrl());
		}
	}
}