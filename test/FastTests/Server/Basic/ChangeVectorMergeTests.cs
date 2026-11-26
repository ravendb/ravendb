using System.Collections.Generic;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Basic
{
    public class ChangeVectorMergeTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private const string Db1 = "aaaaaaaaaaaaaaaaaaaaaa";
        private const string Db2 = "bbbbbbbbbbbbbbbbbbbbbb";
        private const string Db3 = "cccccccccccccccccccccc";

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeKeepsTheHighestEtagOfEveryDatabaseAndAddsTheOthers()
        {
            var merged = ChangeVectorUtils.MergeVectors($"A:5-{Db1}, B:3-{Db2}", $"C:7-{Db1},B:1-{Db2}, D:2-{Db3}");

            Assert.Equal($"C:7-{Db1}, B:3-{Db2}, D:2-{Db3}", merged);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeDownKeepsTheLowestEtagOfTheDatabasesOfTheFirstVector()
        {
            var merged = ChangeVectorUtils.MergeVectorsDown([$"A:5-{Db1}, B:3-{Db2}", $"C:2-{Db1}, D:9-{Db3}"]);

            Assert.Equal($"C:2-{Db1}, B:3-{Db2}", merged);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void MergeDownOfVectorsWithoutACommonDatabaseIsNull()
        {
            Assert.Null(ChangeVectorUtils.MergeVectorsDown(new List<string> { $"A:5-{Db1}", $"D:9-{Db3}" }));
        }
    }
}
