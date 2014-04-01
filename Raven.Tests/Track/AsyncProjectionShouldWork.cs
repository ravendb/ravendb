using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Server;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Track
{
	/// <summary>
	/// Issue RavenDB-191
	/// http://issues.hibernatingrhinos.com/issue/RavenDB-191
	/// </summary>
	public class AsyncProjectionShouldWork : RavenTest, IDisposable
	{
		private readonly RavenDbServer ravenDbServer;
		private readonly IDocumentStore store;

		public AsyncProjectionShouldWork()
		{
			ravenDbServer = GetNewServer();
			store = new DocumentStore { Url = "http://localhost:8079" }.Initialize();
			new TestObjs_Summary().Execute(store);

			using (var session = store.OpenSession())
			{
				session.Store(new TestObj { Name = "Doc1" });
				session.Store(new TestObj { Name = "Doc2" });
				session.SaveChanges();
			}
		}

		public override void Dispose()
		{
			store.Dispose();
			ravenDbServer.Dispose();
			base.Dispose();
		}

		public class TestObj
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Summary
		{
			public string MyId { get; set; }
			public string MyName { get; set; }
		}

		public class TestObjs_Summary : AbstractIndexCreationTask<TestObj, Summary>
		{
			public TestObjs_Summary()
			{
				Map = docs => docs.Select(d => new { MyId = d.Id, MyName = d.Name });

				Store(x => x.MyId, FieldStorage.Yes);
				Store(x => x.MyName, FieldStorage.Yes);
			}
		}

		[Fact]
		public void SyncWorks()
		{
			using (var session = store.OpenSession())
			{
				var q = session.Query<Summary>("TestObjs/Summary")
					.Customize(x => x.WaitForNonStaleResultsAsOfNow())
					.AsProjection<Summary>()
					.ToList();

				AssertResult(q);
			}
		}

		[Fact]
		public void AsyncShouldWorkToo()
		{
			using (var session = store.OpenAsyncSession())
			{
				var q = session.Query<Summary>("TestObjs/Summary")
					.Customize(x => x.WaitForNonStaleResultsAsOfNow())
					.AsProjection<Summary>()
					.ToListAsync();

				q.ContinueWith(task =>
				               	{
				               		try
				               		{
				               			Assert.False(task.IsFaulted);
				               		}
				               		catch (Exception)
				               		{
				               			Console.WriteLine(task.Exception.ExtractSingleInnerException().ToString());
				               			throw;
				               		}
				               		AssertResult(task.Result);
				               	}).Wait();
			}
		}

		private void AssertResult(IList<Summary> q)
		{
			Assert.Equal(2, q.Count);

			for (var i = 1; i < q.Count; i++)
			{
				Assert.NotNull(q[i].MyId);
				Assert.True(q[i].MyName.StartsWith("Doc"));
			}
		}
	}
}