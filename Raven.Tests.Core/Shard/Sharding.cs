using Raven.Client;
using Raven.Client.Shard;
using Raven.Tests.Core.Replication;
using Raven.Tests.Core.Utils.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.Shard
{
    public class Sharding : RavenReplicationCoreTest
    {
        [Fact]
        public void CanUseShardedDocumentStore()
        {
            const string shard1Name = "Asia";
            const string shard2Name = "Middle East";
            const string shard3Name = "America";

            using (var shard1 = GetDocumentStore())
            using (var shard2 = GetDocumentStore())
            using (var shard3 = GetDocumentStore())
            {
                var shards = new Dictionary<string, IDocumentStore>
                {
                    {shard1Name, shard1},
                    {shard2Name, shard2},
                    {shard3Name, shard3}
                };

                var shardStrategy = new ShardStrategy(shards)
                    .ShardingOn<Company>(company => company.Name, result =>
                    {
                        if (ReferenceEquals(result, null))
                            throw new InvalidOperationException("Should not be null.");

                        char firstCharacter = result.ToString().First();
                        if (char.ToLower(firstCharacter) == 'a')
                        {
                            return shard1Name;
                        }
                        else if (char.ToLower(firstCharacter) == 'b')
                        {
                            return shard2Name;
                        }
                        else
                        {
                            return shard3Name;
                        }
                    });

                using (var documentStore = new ShardedDocumentStore(shardStrategy).Initialize())
                {
                    using (var session = documentStore.OpenSession())
                    {
                        session.Store(new Company { Name = "A corporation" });
                        session.Store(new Company { Name = "alimited" });
                        session.Store(new Company { Name = "B corporation" });
                        session.Store(new Company { Name = "blimited" });
                        session.Store(new Company { Name = "Z corporation" });
                        session.Store(new Company { Name = "rlimited" });
                        session.SaveChanges();

                        var companies = session.Query<Company>()
                            .ToArray();

                        Assert.Equal(6, companies.Length);
                        Assert.Equal(shard1Name + "/companies/1", companies[0].Id);
                        Assert.Equal(shard1Name + "/companies/2", companies[1].Id);
                        Assert.Equal(shard2Name + "/companies/3", companies[2].Id);
                        Assert.Equal(shard2Name + "/companies/4", companies[3].Id);
                        Assert.Equal(shard3Name + "/companies/5", companies[4].Id);
                        Assert.Equal(shard3Name + "/companies/6", companies[5].Id);

                        Assert.Throws<InvalidOperationException>(() =>
                            {
                                session.Store(new Company { });
                                session.SaveChanges();
                            });
                    }
                }
            }
        }
    }
}
