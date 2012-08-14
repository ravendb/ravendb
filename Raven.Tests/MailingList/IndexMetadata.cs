using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class IndexMetadata : RavenTest
	{
		public class Users_DeleteStatus : AbstractMultiMapIndexCreationTask
		{
			public Users_DeleteStatus()
			{
				AddMap<User>(users => from user in users
				                      select new
				                      {
										  Deleted = MetadataFor(user)["Deleted"]
				                      });
			}
		}

		[Fact]
		public void WillGenerateProperIndex()
		{
			var usersDeleteStatus = new Users_DeleteStatus {Conventions = new DocumentConvention()};
			var indexDefinition = usersDeleteStatus.CreateIndexDefinition();
			Assert.Contains("Deleted = user[\"@metadata\"][\"Deleted\"]", indexDefinition.Map);
		}

		[Fact]
		public void CanCreateIndex()
		{
			using(var store = NewDocumentStore())
			{
				new Users_DeleteStatus().Execute(store);
			}
		}
	}
}