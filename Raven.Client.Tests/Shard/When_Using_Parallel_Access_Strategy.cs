using System;
using Raven.Client.Document;
using Raven.Client.Tests.Document;
using Xunit;
using System.Collections.Generic;
using Rhino.Mocks;
using Raven.Client.Shard.ShardStrategy.ShardAccess;
using System.Linq;

namespace Raven.Client.Tests.Shard
{
    public class When_Using_Parallel_Access_Strategy : BaseTest
	{
        [Fact]
        public void Can_get_complete_result_list()
        {
            var shard1 = MockRepository.GenerateStub<IDocumentSession>();
            shard1.Stub(x => x.Query<Company>())
                .Return(new[] { new Company { Name = "Company1" } }.AsQueryable());

            var shard2 = MockRepository.GenerateStub<IDocumentSession>();
			shard2.Stub(x => x.Query<Company>())
				.Return(new[] { new Company { Name = "Company2" } }.AsQueryable());

			var results = new ParallelShardAccessStrategy().Apply(new[] { shard1, shard2 }, x => x.Query<Company>().ToArray());

            Assert.Equal(2, results.Count);
        }

        [Fact]
        public void Null_result_is_not_an_exception()
        {
            var shard1 = MockRepository.GenerateStub<IDocumentSession>();

			var results = new ParallelShardAccessStrategy().Apply(new[] { shard1 }, x => (IList<Company>)null);

            Assert.Equal(0, results.Count);
        }

        [Fact]
        public void Execution_exceptions_are_rethrown()
        {
            var shard1 = MockRepository.GenerateStub<IDocumentSession>();
			shard1.Stub(x => x.Query<Company>()).Throw(new ApplicationException("Oh noes!"));

            Assert.Throws(typeof(ApplicationException), () =>
            {
                new ParallelShardAccessStrategy().Apply(new[] { shard1 }, x => x.Query<Company>().ToArray());
            });
        }
    }
}