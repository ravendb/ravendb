using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Database.Smuggler.Embedded;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Streams;
using Raven.Tests.Common;

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
				using (var stream = typeof(BigDoc).Assembly.GetManifestResourceStream("Raven.Tests.Patching.failingdump11.ravendump"))
				{
				    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerStreamSource(stream),
                        new DatabaseSmugglerEmbeddedDestination(store.SystemDatabase));

				    smuggler.Execute();
	            }

	            using (var session = store.OpenSession())
	            {
                    session.Advanced.DocumentQuery<object>("Raven/DocumentsByEntityName").WaitForNonStaleResults().ToList();

	                store.DatabaseCommands.UpdateByIndex("Raven/DocumentsByEntityName", new IndexQuery {Query = "Tag:Regions"},
	                    new ScriptedPatchRequest
	                    {
	                        Script = @"this.Test = 'test';"
	                    }
	                    , new BulkOperationOptions {AllowStale = true, MaxOpsPerSec = null,StaleTimeout = null});
	            }
	        }
	    }
	}
}