using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Client.Document;
using Raven.Database;
using Raven.Server;
using Xunit;
using System.Collections.Generic;
using Raven.Client.Shard;
using Rhino.Mocks;
using Raven.Client.Shard.ShardStrategy.ShardAccess;

namespace Raven.Client.Tests
{
    public class When_Using_Parallel_Access_Strategy : BaseTest
	{
        [Fact]
        public void Can_get_complete_result_list()
        {
            var shard1 = MockRepository.GenerateStub<IDocumentSession>();
            shard1.Stub(x => x.GetAll<Company>())
                //.Callback(() => { Thread.Sleep(500); return true; })
                .Return(new[] { new Company { Name = "Company1" } });

            var shard2 = MockRepository.GenerateStub<IDocumentSession>();
            shard2.Stub(x => x.GetAll<Company>())
                //.Callback(() => { Thread.Sleep(100); return true; })
                .Return(new[] { new Company { Name = "Company2" } });

            var results = new ParallelShardAccessStrategy().Apply(new[] { shard1, shard2 }, x => x.GetAll<Company>());

            Assert.Equal(2, results.Count);
        }

        [Fact]
        public void Null_result_is_not_an_exception()
        {
            var shard1 = MockRepository.GenerateStub<IDocumentSession>();
            shard1.Stub(x => x.GetAll<Company>()).Return(null);

            var results = new ParallelShardAccessStrategy().Apply(new[] { shard1 }, x => x.GetAll<Company>());

            Assert.Equal(0, results.Count);
        }

        [Fact]
        public void Execution_exceptions_are_rethrown()
        {
            var shard1 = MockRepository.GenerateStub<IDocumentSession>();
            shard1.Stub(x => x.GetAll<Company>()).Throw(new ApplicationException("Oh noes!"));

            Assert.Throws(typeof(ApplicationException), () =>
            {
                new ParallelShardAccessStrategy().Apply(new[] { shard1 }, x => x.GetAll<Company>());
            });
        }
    }
}