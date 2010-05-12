using System;
using Raven.Client.Document;
using Raven.Client.Tests.Document;
using Xunit;
using System.Collections.Generic;
using Raven.Client.Shard.ShardStrategy.ShardAccess;
using System.Linq;

namespace Raven.Client.Tests.Shard
{
    public class When_Using_Parallel_Access_Strategy : BaseTest
	{

        [Fact]
        public void Null_result_is_not_an_exception()
        {
        	var shard1 = new DocumentStore {Url = "http://localhost:8080"}.Initialise().OpenSession();

			var results = new ParallelShardAccessStrategy().Apply(new[] { shard1 }, x => (IList<Company>)null);

            Assert.Equal(0, results.Count);
        }

        [Fact]
        public void Execution_exceptions_are_rethrown()
        {
            var shard1 = new DocumentStore { Url = "http://localhost:8080" }.Initialise().OpenSession();


            Assert.Throws(typeof(ApplicationException), () =>
            {
            	new ParallelShardAccessStrategy().Apply<object>(new[] {shard1}, x => { throw new ApplicationException(); });
            });
        }
    }
}