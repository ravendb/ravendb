﻿using System;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Utils;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server
{
    public class ThreadUsageTests : RavenLowLevelTestBase
    {
        public ThreadUsageTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.None, RavenPlatform.Windows | RavenPlatform.Linux)]
        public void ThreadUsage_WhenThreadsHaveSameCpuUsageAndTotalProcessorTime_ShouldListThemBoth()
        {
            using var database = CreateDocumentDatabase();

            using var index1 = MapIndex.CreateNew(new IndexDefinition {Name = "Companies_ByName", Maps = { "from company in docs.Companies select new { company.Name }" },}, database);
            using var index2 = MapIndex.CreateNew(new IndexDefinition {Name = "Users_ByName", Maps = {"from user in docs.Orders select new { user.Name }"},}, database);
            using var index3 = MapIndex.CreateNew(new IndexDefinition {Name = "Orders_ByName", Maps = { "from order in docs.Orders select new { order.Name }" },}, database);

            index1.Start();
            index2.Start();
            index3.Start();


            for (int i = 0;; i++)
            {
                try
                {
                    var threadsUsage = new ThreadsUsage();
                    var threadsInfo = threadsUsage.Calculate();
                    var threadNames = threadsInfo.List.Select(ti => ti.Name).OrderBy(n => n).ToArray();

                    ThreadNames.FullThreadNames.TryGetValue(index1._indexingThread.ManagedThreadId, out var index1FullName);
                    ThreadNames.FullThreadNames.TryGetValue(index2._indexingThread.ManagedThreadId, out var index2FullName);
                    ThreadNames.FullThreadNames.TryGetValue(index3._indexingThread.ManagedThreadId, out var index3FullName);

                    RavenTestHelper.AssertAll(() => string.Join('\n', threadNames.Select(s => $"\"{s}\"")),
                        () => AssertContains(index1FullName),
                        () => AssertContains(index2FullName),
                        () => AssertContains(index3FullName));

                    break;
                    void AssertContains(string threadName) => Assert.True(threadNames.Contains(threadName), $"Not found : {threadName}");
                }

                catch
                {
                    if(i >= 5)
                        throw;
                    Thread.Sleep(100);
                }
            }
        }
    }
}
