using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class CanDeleteIndex : RavenTest
	{
		private class AllDocs : AbstractIndexCreationTask<object>
		{
			public AllDocs() { Map = docs => from doc in docs select new { }; }
		}
            
		[Fact]
		public void WithNoErrors()
		{
			using(GetNewServer())
			using(var docStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				new AllDocs().Execute(docStore);
				docStore.DatabaseCommands.DeleteIndex("AllDocs");
			}
		}
	}
}