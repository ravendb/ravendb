namespace Raven.Studio.Database
{
	using System.Collections.Generic;
	using System.Linq;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Framework;
	using Plugin;

	public class SummaryViewModel : Screen, IRavenScreen
	{
		readonly TemplateColorProvider colorProvider;
		readonly IServer server;
		readonly DocumentTemplateProvider templateProvider;

		public SummaryViewModel(IServer server, TemplateColorProvider colorProvider)
		{
			this.server = server;
			this.colorProvider = colorProvider;
			this.templateProvider = new DocumentTemplateProvider(server, colorProvider);
		}

		public string DatabaseName
		{
			get { return server.CurrentDatabase; }
		}

		public IEnumerable<DocumentViewModel> RecentDocuments { get; private set; }
		public IEnumerable<Collection> Collections { get; private set; }

		public SectionType Section
		{
			get { return SectionType.Documents; }
		}

		protected override void OnActivate()
		{
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetCollectionsAsync(0, 25)
					.ContinueOnSuccess(x =>
					                   	{
					                   		Collections = x.Result;
					                   		NotifyOfPropertyChange(() => Collections);
					                   	});

				session.Advanced.AsyncDatabaseCommands
					.GetDocumentsAsync(0, 12)
					.ContinueOnSuccess(x =>
					                   	{
					                   		RecentDocuments =
					                   			x.Result.Select(doc => new DocumentViewModel(doc, templateProvider)).ToArray();
					                   		NotifyOfPropertyChange(() => RecentDocuments);
					                   	});
			}
		}
	}
}