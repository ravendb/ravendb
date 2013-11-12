using System;
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
	            using (var stream = typeof (TroyMapReduceImport).Assembly.GetManifestResourceStream("Raven.Tests.Patching.failingdump11.ravendump"))
	            {
	                var smugglerOptions = new SmugglerOptions {BackupStream = stream};
	                new DataDumper(store.DocumentDatabase).ImportData(smugglerOptions).Wait(TimeSpan.FromSeconds(15));
	            }

	            using (var session = store.OpenSession())
	            {
	                session.Advanced.LuceneQuery<object>("Raven/DocumentsByEntityName").WaitForNonStaleResults().ToList();

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