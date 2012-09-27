using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Smuggler;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Patching
{
	public class BigDoc : RavenTest
	{
		[Fact]
		public void CanGetCorrectResult()
		{
			using (var store = NewDocumentStore())
			{
				var smugglerOptions = new SmugglerOptions();
				var dataDumper = new DataDumper(store.DocumentDatabase, smugglerOptions);
				using (var stream = typeof(TroyMapReduceImport).Assembly.GetManifestResourceStream("Raven.Tests.Patching.failingdump11.ravendump"))
				{
					dataDumper.ImportData(stream, smugglerOptions);
				}

				
				using (var s = store.OpenSession())
				{
					s.Advanced.LuceneQuery<object>("Raven/DocumentsByEntityName").WaitForNonStaleResults().ToList();

					store.DatabaseCommands.UpdateByIndex("Raven/DocumentsByEntityName", new IndexQuery {Query = "Tag:Regions"},
					                                     new ScriptedPatchRequest
					                                     {
						                                     Script = @"this.Test = 'test';"
					                                     }
					                                     , true);
				}
			}
		}
	}
}