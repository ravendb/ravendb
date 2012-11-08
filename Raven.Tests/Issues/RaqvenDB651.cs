using System;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RaqvenDB651 : RavenTest
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

				WaitForUserToContinueTheTest(documentStore);

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
	}
}