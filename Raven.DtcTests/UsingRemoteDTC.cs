using System.Transactions;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs.DTC
{
	public class UsingRemoteDTC : RavenTest
	{
		[Fact]
		public void CanUseRemoteDTC()
		{
            using (var server = GetNewServer(requestedStorage: "esent"))
			{
                EnsureDtcIsSupported(server);
			    
				using(var store = new DocumentStore{ Url = "http://localhost:8079"}.Initialize())
				{
					User u2;
					bool happy;

					using (var tx2 = new TransactionScope())
					using (var s2 = store.OpenSession())
					{
						u2 = new User { Email = "b@b" };
						s2.Store(u2);
						s2.SaveChanges();
						tx2.Complete();
					}

					using (var s3 = store.OpenSession())
					{
						s3.Advanced.AllowNonAuthoritativeInformation = false;
						happy = s3.Load<User>(u2.Id) != null;
					}

					Assert.True(happy);
				}
			}	
		}
	}
}