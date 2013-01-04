using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Indexes
{
	public class AnalyzerResolution : RavenTest
	{
		public void can_resolve_internal_analyzer()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinitionBuilder<Bugs.Patching.Post>
															{
																Map = docs => from doc in docs select new { doc.Id },
																Analyzers = { { x => x.Id, "SimpleAnalyzer" } }
															});
			}
		}
	}
}
