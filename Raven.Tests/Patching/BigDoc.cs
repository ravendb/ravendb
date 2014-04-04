using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Database.Smuggler;
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
	                new DataDumper(store.DocumentDatabase).ImportData(new SmugglerImportOptions {FromStream = stream}, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));
	            }

	            using (var session = store.OpenSession())
	            {
                    session.Advanced.DocumentQuery<object>("Raven/DocumentsByEntityName").WaitForNonStaleResults().ToList();

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