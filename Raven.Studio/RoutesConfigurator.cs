using Raven.Studio.Features.Collections;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure.Navigation;

namespace Raven.Studio
{
	public class RoutesConfigurator
	{
		public void Configure()
		{
			var routes = NavigationService.Routes;
			routes.Add(typeof(CollectionsViewModel), "/{database}/collections");
			routes.Add(typeof(EditDocumentViewModel), "/{database}/documents/edit/{*documentId}");
		}
	}
}