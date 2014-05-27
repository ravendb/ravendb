using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class DerekReplication
	{
	    [Fact]
	    public void DocumentStartingWith_should_work()
	    {
		    using (var store = new DocumentStore
		    {
			    Url = "http://localhost:8080",
				DefaultDatabase = "EasyFlor"
		    })
		    {
			    store.Initialize();
				var entityMappings1 = store.DatabaseCommands.StartsWith("EntityMappings/PackagingSort/", null, 0, 128);
				var entityMappings2 = store.DatabaseCommands.StartsWith("EntityMappings/VBNArticle/", null, 0, 128);

			    using (var session = store.OpenSession())
			    {
				    var docs1 = session.Advanced.LoadStartingWith<dynamic>("EntityMappings/PackagingSort/");
					var docs2 = session.Advanced.LoadStartingWith<dynamic>("EntityMappings/VBNArticle/");
			    }

		    }
	    }
	}
}
