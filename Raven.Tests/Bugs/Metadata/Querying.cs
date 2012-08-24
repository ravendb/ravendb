using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Metadata
{
	public class Querying : RavenTest
	{
		[Fact]
		public void Can_query_metadata()
		{
			using(var DocStore = NewDocumentStore())
			{
				var user1 = new User { Name = "Joe Schmoe" };
				// This test succeeds if I use "Test-Property1" as the  property name.
				const string propertyName1 = "Test-Property-1";
				const string propertyValue1 = "Test-Value-1";
				using (var session = DocStore.OpenSession())
				{
					session.Store(user1);
					var metadata1 = session.Advanced.GetMetadataFor(user1);
					metadata1[propertyName1] = propertyValue1;
					session.Store(new User { Name = "Ralph Schmoe" });
					session.SaveChanges();
				}

				using (var session = DocStore.OpenSession())
				{
					var result = session.Advanced.LuceneQuery<User>()
						.WaitForNonStaleResultsAsOfNow()
						.WhereEquals("@metadata." + propertyName1, propertyValue1)
						.ToList();

					Assert.NotNull(result);
					Assert.Equal(1, result.Count);
					var metadata = session.Advanced.GetMetadataFor(result[0]);
					Assert.Equal(propertyValue1, metadata[propertyName1]);
				}
			}
		}

		[Fact]
		public void Index_should_take_into_account_number_of_dashes()
		{
			using (var DocStore = NewDocumentStore())
			{
				var user1 = new User { Name = "Joe Schmoe" };
				using (var session = DocStore.OpenSession())
				{
					session.Store(user1);
					var metadata1 = session.Advanced.GetMetadataFor(user1);
					metadata1["Test-Property-1"] = "Test-Value-1";
					session.Store(new User { Name = "Ralph Schmoe" });
					session.SaveChanges();
				}

				using (var session = DocStore.OpenSession())
				{
					Assert.Empty(session.Advanced.LuceneQuery<User>()
					             	.WaitForNonStaleResultsAsOfNow()
					             	.WhereEquals("@metadata." + "Test-Property1", "Test-Value-1")
					             	.ToList());

					var result = session.Advanced.LuceneQuery<User>()
						.WaitForNonStaleResultsAsOfNow()
						.WhereEquals("@metadata." + "Test-Property-1", "Test-Value-1")
						.ToList();

					Assert.NotNull(result);
					Assert.Equal(1, result.Count);
					var metadata = session.Advanced.GetMetadataFor(result[0]);
					Assert.Equal("Test-Value-1", metadata["Test-Property-1"]);
				}
			}
		}
	}
}