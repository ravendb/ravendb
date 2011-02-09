namespace Raven.Studio.Plugins.Linq
{
	using System;
	using System.Collections.Generic;
	using Caliburn.Micro;
	using Documents;
	using Plugin;

	public class LinqEditorViewModel : Screen, IRavenScreen
	{
		string query;
		IList<DocumentViewModel> results;

		public LinqEditorViewModel(IServer server)
		{
			Server = server;
		}

		public IServer Server { get; private set; }

		public IList<DocumentViewModel> Results
		{
			get { return results; }
			set
			{
				results = value;
				NotifyOfPropertyChange(() => Results);
			}
		}

		public string Query
		{
			get { return query; }
			set
			{
				query = value;
				NotifyOfPropertyChange(() => Query);
			}
		}

		public SectionType Section
		{
			get { return SectionType.Linq; }
		}

		public void Execute()
		{
			throw new NotImplementedException();
			//if (!string.IsNullOrWhiteSpace(Query))
			//{
			//    Database.IndexSession.LinearQuery(Query, 0, 25,
			//                                      o =>
			//                                          {
			//                                              Results =
			//                                                  o.Data.Select(x => new DocumentViewModel(new Document(x), Database, this)).ToArray();
			//                                          });
			//}
		}
	}
}