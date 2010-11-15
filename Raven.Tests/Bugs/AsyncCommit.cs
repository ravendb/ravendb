using System.Transactions;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class AsyncCommit : LocalClientTest
	{
		[Fact]
		public void DtcCommitWillGiveNewResultIfNonAuthoritiveIsSetToFalse()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var s = documentStore.OpenSession())
				{
					s.Store(new AccurateCount.User { Name = "Ayende" });
					s.SaveChanges();
				}

				using (var s = documentStore.OpenSession())
				using (var scope = new TransactionScope())
				{
					var user = s.Load<AccurateCount.User>("users/1");
					user.Name = "Rahien";
					s.SaveChanges();
					scope.Complete();
				}

				using (var s = documentStore.OpenSession())
				{
                    s.Advanced.AllowNonAuthoritiveInformation = false;
					var user = s.Load<AccurateCount.User>("users/1");
					Assert.Equal("Rahien", user.Name);
				}
			}
		}

        [Fact]
        public void DtcCommitWillGiveNewResultIfNonAuthoritiveIsSetToFalseWhenQuerying()
        {
            using (var documentStore = NewDocumentStore())
            {
                documentStore.DatabaseCommands.PutIndex("test",
                                                        new IndexDefinition
                                                        {
                                                            Map = "from doc in docs select new { doc.Name }"
                                                        });

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new AccurateCount.User { Name = "Ayende" });
                    s.SaveChanges();

                    s.Advanced.LuceneQuery<AccurateCount.User>("test")
                        .WaitForNonStaleResults()
                        .FirstOrDefault();
                }

                using (var s = documentStore.OpenSession())
                using (var scope = new TransactionScope())
                {
                    var user = s.Load<AccurateCount.User>("users/1");
                    user.Name = "Rahien";
                    s.SaveChanges();
                    scope.Complete();
                }


                using (var s = documentStore.OpenSession())
                {
                    s.Advanced.AllowNonAuthoritiveInformation = false;
                    var user = s.Advanced.LuceneQuery<AccurateCount.User>("test")
                        .FirstOrDefault();
                    Assert.Equal("Rahien", user.Name);
                }
            }
        }
	}
}
