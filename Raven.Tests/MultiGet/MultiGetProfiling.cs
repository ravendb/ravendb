using System;
using System.Linq;
using Raven.Client.Debug;
using Raven.Client.Linq;
using Raven.Client.Document;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.MultiGet
{
	public class MultiGetProfiling : RemoteClientTest
	{
		[Fact]
		public void CanProfileLazyRequests()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" })
			{
				store.Initialize();
				using (var session = store.OpenSession())
				{
					// handle the initial request for replication information
				}
				Guid id;
				using (var session = store.OpenSession())
				{
					id = session.Advanced.DatabaseCommands.ProfilingInformation.Id;
					session.Advanced.Lazily.Load<User>("users/1");
					session.Advanced.Lazily.Load<User>("users/2");
					session.Advanced.Lazily.Load<User>("users/3");

					session.Advanced.Lazily.ExecuteAllPendingLazyOperations();
				}

				var profilingInformation = store.GetProfilingInformationFor(id);
				Assert.Equal(1, profilingInformation.Requests.Count);

			}
		}

		//[Fact]
		//public void CanProfilePartiallyCachedLazyRequest()
		//{
		//    using (GetNewServer())
		//    using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
		//    {
		//        using (var session = store.OpenSession())
		//        {
		//            session.Store(new User { Name = "oren" });
		//            session.Store(new User());
		//            session.Store(new User { Name = "ayende" });
		//            session.Store(new User());
		//            session.SaveChanges();
		//        }

		//        using (var session = store.OpenSession())
		//        {
		//            session.Query<User>()
		//                .Customize(x => x.WaitForNonStaleResults())
		//                .Where(x => x.Name == "test")
		//                .ToList();
		//        }


		//        using (var session = store.OpenSession())
		//        {
		//            session.Query<User>().Where(x => x.Name == "oren").ToArray();
		//            session.Query<User>().Where(x => x.Name == "ayende").ToArray();
		//        }

		//        using (var session = store.OpenSession())
		//        {
		//            var result1 = session.Query<User>().Where(x => x.Name == "oren").Lazily();
		//            var result2 = session.Query<User>().Where(x => x.Name == "ayende").Lazily();
		//            Assert.NotEmpty(result2.Value);

		//            Assert.Equal(1, session.Advanced.NumberOfRequests);
		//            Assert.NotEmpty(result1.Value);
		//            Assert.Equal(1, session.Advanced.NumberOfRequests);
		//            Assert.Equal(2, store.JsonRequestFactory.NumberOfCachedRequests);
		//        }
		//    }
		//}
	}
}