using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class IssueTest : RavenTestBase
	{
	    public class SagaUniqueIdentity
	    {
	        public string Id { get; set; }
	        public string SagaDocId { get; set; }
	    }

        public class Saga
        {
            public string Id { get; set; }
        }

	    [Fact]
		public void WillSupportLast()
		{
            using (var store = NewRemoteDocumentStore())
			{
                //If you remove the '+' sign this test passes
                var DocIdWithPlusSign = "VideoStore.Sales.ProcessOrderSaga+OrderData/OrderNumber/3842cac4-b9a0-8223-0dcc-509a6f75849b";

				using (var session = store.OpenSession())
				{
                    session.Store(new Saga());

                    session.Store(new SagaUniqueIdentity { Id = DocIdWithPlusSign, SagaDocId = "sagas/1" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
                    var doc = session
                        .Include("SagaDocId")
                        .Load<SagaUniqueIdentity>(DocIdWithPlusSign);

                    Assert.NotNull(doc);
				    var saga = session.Load<Saga>(doc.SagaDocId);
                    Assert.NotNull(saga);
				}
			}
		}
	}
}