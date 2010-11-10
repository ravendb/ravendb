using System;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Http;
using Raven.Tests.Document;
using Xunit;
using System.Collections.Generic;
using Raven.Client.Shard.ShardStrategy.ShardAccess;

namespace Raven.Tests.Shard
{
    public class When_Using_Parallel_Access_Strategy  : RemoteClientTest, IDisposable
	{
		private readonly string path;
        private readonly int port;

		public When_Using_Parallel_Access_Strategy()
		{
            port = 8080;
            path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
		}

		#region IDisposable Members

		public void Dispose()
		{
            IOExtensions.DeleteDirectory(path);
		}

		#endregion

        [Fact]
        public void Null_result_is_not_an_exception()
        {
        	using(GetNewServer(port, path))
        	{
                var shard1 = new DocumentStore { Url = "http://localhost:8080" }.Initialize().OpenSession();

                var results = new ParallelShardAccessStrategy().Apply(new[] { shard1 }, x => (IList<Company>)null);

                Assert.Equal(0, results.Count);
        	}
        }

        [Fact]
        public void Execution_exceptions_are_rethrown()
        {
            using (GetNewServer(port, path))
            {
                var shard1 = new DocumentStore {Url = "http://localhost:8080"}.Initialize().OpenSession();


                Assert.Throws(typeof (ApplicationException), () =>
                {
                    new ParallelShardAccessStrategy().Apply<object>(new[] {shard1},
                                                                    x => { throw new ApplicationException(); });
                });
            }
        }
	}
}
