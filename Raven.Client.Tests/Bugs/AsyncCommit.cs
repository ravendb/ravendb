using System.Transactions;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class AsyncCommit : LocalClientTest
	{
		[Fact]
		public void DtcCommitWillGiveOldResult()
		{
			using(var documentStore = NewDocumentStore())
			{
				using(var s = documentStore.OpenSession())
				{
					s.Store(new AccurateCount.User{ Name = "Ayende"});	
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
					var user = s.Load<AccurateCount.User>("users/1");
					Assert.Equal("Ayende", user.Name);
				}
			}
		}

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
					s.AllowNonAuthoritiveInformation = false;
					var user = s.Load<AccurateCount.User>("users/1");
					Assert.Equal("Rahien", user.Name);
				}
			}
		}
	}
}