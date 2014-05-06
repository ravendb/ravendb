using System;
using Raven.Client;
using Raven.Client.Linq;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class LastModifiedMetadataTest : RavenTest
	{

		private class AmazingIndex2 : AbstractIndexCreationTask<User>
		{
			public class ModifiedDocuments
			{
				public string InternalId { get; set; }
				public DateTime LastModified { get; set; }
			}

			public AmazingIndex2()
			{
				Map = docs =>
					  from doc in docs
					  select new
					  {
						  LastModified = MetadataFor(doc)["Last-Modified"],
					  };
				TransformResults = (database, results) => from doc in results
														  select new
														  {
															  InternalId = doc.InternalId,
															  LastModified = MetadataFor(doc)["Last-Modified"],
														  };
			}
		}


		private class User
		{
			public string InternalId { get; set; }
			public string Name { get; set; }
		}


		[Fact]
		public void Can_index_and_query_metadata2()
		{
			using(GetNewServer())
			using(var DocStore = new DocumentStore
			{
				Conventions =
					{
						FindIdentityProperty = info => info.Name == "InternalId"
					},
				Url = "http://localhost:8079"
			}.Initialize())
			{
				var user1 = new User { Name = "Joe Schmoe" };
				var user2 = new User { Name = "Jack Spratt" };
				new AmazingIndex2().Execute(DocStore);
				using (var session = DocStore.OpenSession())
				{
					session.Store(user1);
					session.Store(user2);
					session.SaveChanges();
				}
				using (var session = DocStore.OpenSession())
				{
					var user3 = session.Load<User>(user1.InternalId);
					Assert.NotNull(user3);
					var metadata = session.Advanced.GetMetadataFor(user3);
					var lastModified = metadata.Value<DateTime>("Last-Modified");

					var modifiedDocuments = (from u in session.Query<AmazingIndex2.ModifiedDocuments, AmazingIndex2>()
												 .Customize(x=>x.WaitForNonStaleResults())
											 orderby u.InternalId
											 select u)
											 .As<AmazingIndex2.ModifiedDocuments>().ToList();

					Assert.Equal(2, modifiedDocuments.Count);
					Assert.Equal(user1.InternalId, modifiedDocuments[0].InternalId);

					Assert.Equal(lastModified.ToString("yyyy-MM-dd hh:mm:ss"), modifiedDocuments[0].LastModified.ToString("yyyy-MM-dd hh:mm:ss"));

				}
			}
		} 
	}
}