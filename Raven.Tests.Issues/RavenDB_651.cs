using System;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_651 : RavenTest
	{
		[Fact]
		public void Can_Save_And_Load_DateTimeOffset_From_Metadata()
		{
			using (var documentStore = NewDocumentStore())
			{
				var dto = new DateTimeOffset(2012, 1, 1, 8, 0, 0, TimeSpan.FromHours(-2));
				using (var session = documentStore.OpenSession())
				{
					var foo = new Foo { Id = "foos/1" };
					session.Store(foo);
					var metadata = session.Advanced.GetMetadataFor(foo);
					metadata.Add("TestDTO", dto); 
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var foo = session.Load<Foo>("foos/1");
					var metadata = session.Advanced.GetMetadataFor(foo);
					var testDto = metadata.Value<DateTimeOffset>("TestDTO");
					Assert.Equal(dto, testDto);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
		}

		[Fact]
		public void CanWorkWithDateTimeOffset()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = @"from doc in docs select new { Time = doc[""@metadata""].time }"
				});

				using (var session = store.OpenSession())
				{
					var entity = new User();
					session.Store(entity);
					session.Advanced.GetMetadataFor(entity)["time"] = new DateTimeOffset(2012, 11, 08, 11, 20, 0, TimeSpan.FromHours(2));
					session.SaveChanges();
				}

				WaitForIndexing(store);
				WaitForUserToContinueTheTest(store);
				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}
	}
}