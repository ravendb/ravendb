using System;
using Raven.Abstractions;
using Raven.Client;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB845 : RavenTestBase
	{
		public override void Dispose()
		{
			SystemTime.UtcDateTime = null;
			base.Dispose();
		}

		public class Foo
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void LastModifiedDate_IsUpdated_Local()
		{
			using (var documentStore = NewDocumentStore(requestedStorage: "esent"))
			{
				DoTest(documentStore);
			}
		}

		[Fact]
		public void LastModifiedDate_IsUpdated_Remote()
		{
			using (var documentStore = NewRemoteDocumentStore())
			{
				DoTest(documentStore);
			}
		}

		private void DoTest(IDocumentStore documentStore)
		{
			using (var session = documentStore.OpenSession())
			{
				session.Store(new Foo { Id = "foos/1", Name = "A" });
				session.SaveChanges();
			}

			DateTime firstDate;
			using (var session = documentStore.OpenSession())
			{
				var foo = session.Load<Foo>("foos/1");
				var metadata = session.Advanced.GetMetadataFor(foo);
				firstDate = metadata.Value<DateTime>("Last-Modified");
			}

			SystemTime.UtcDateTime = () => DateTime.UtcNow.AddDays(1);
			using (var session = documentStore.OpenSession())
			{
				var foo = session.Load<Foo>("foos/1");
				foo.Name = "B";
				session.SaveChanges();
			}
			DateTime secondDate;
			using (var session = documentStore.OpenSession())
			{
				var foo = session.Load<Foo>("foos/1");
				var metadata = session.Advanced.GetMetadataFor(foo);
				secondDate = metadata.Value<DateTime>("Last-Modified");
			}

			Assert.NotEqual(secondDate, firstDate);
		}
	}
}