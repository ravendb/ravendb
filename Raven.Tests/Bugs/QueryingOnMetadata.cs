using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryingOnMetadata : RavenTest
	{
		[Fact]
		public void CanQueryOnNullableProperty()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					var u1 = new User();
					session.Store(u1);
					var metadata = session.Advanced.GetMetadataFor(u1);
					metadata["Errored"] = true;
					metadata["JobId"] = "12cd80f2-34b0-4dd9-8464-d1cefad07256";

					var u2 = new User();
					session.Store(u2);
					var metadata2 = session.Advanced.GetMetadataFor(u2);
					// doesn't have metadata property
					//metadata2["Errored"] = true;
					metadata2["JobId"] = "12cd80f2-34b0-4dd9-8464-d1cefad07256";

					var u3 = new User();
					session.Store(u3);
					var metadata3 = session.Advanced.GetMetadataFor(u3);
					metadata2["Errored"] = false;
					metadata3["JobId"] = "12cd80f2-34b0-4dd9-8464-d1cefad07256";
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var users = session.Advanced.LuceneQuery<User>()
							.WaitForNonStaleResultsAsOfNow()
							.WhereEquals("@metadata.JobId", "12cd80f2-34b0-4dd9-8464-d1cefad07256")
							.AndAlso()
							.WhereEquals("@metadata.Errored", false)
							.ToArray();

					Assert.Equal(1, users.Length);
				}
			}
		}
	}
}
