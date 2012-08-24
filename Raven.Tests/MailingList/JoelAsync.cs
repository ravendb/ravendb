using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class JoelAsync : RavenTest
	{
		public class Dummy
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void AsyncQuery()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					var results = session.Query<Dummy>().ToListAsync();
					results.Wait();

					var results2 = session.Query<Dummy>().ToListAsync();
					results2.Wait();
				
					Assert.Equal(0, results2.Result.Count);
				}
			}
		}
	}
}
