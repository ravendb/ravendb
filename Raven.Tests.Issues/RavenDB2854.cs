// -----------------------------------------------------------------------
//  <copyright file="RavenDB2854.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Document.Async;
using Raven.Database.Server.Controllers;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB2854 : RavenTest
    {
        public class Dog
        {
            public bool Cute { get; set; }
        }

        [Fact]
        public void CanGetCountWithoutGettingAllTheData()
        {
            using (var store = NewDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;
                using (var s = store.OpenSession())
                {
                    id = ((DocumentSession)s).Id;
                    var count = s.Query<Dog>().Count(x => x.Cute);
                    Assert.Equal(0, count);
                }

                var profiling = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profiling.Requests.Count);

                Assert.Contains("pageSize=0", profiling.Requests[0].Url);

            }
        }
        [Fact]
        public async Task CanGetCountWithoutGettingAllTheDataAsync()
        {
            using (var store = NewDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;
                using (var s = store.OpenAsyncSession())
                {
                    id = ((AsyncDocumentSession)s).Id;
                    var count = await s.Query<Dog>().Where(x => x.Cute).CountAsync();
                    Assert.Equal(0, count);
                }

                var profiling = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profiling.Requests.Count);

                Assert.Contains("pageSize=0", profiling.Requests[0].Url);

            }
        }
        [Fact]
        public void CanGetCountWithoutGettingAllTheDataLazy()
        {
            using (var store = NewDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;
                using (var s = store.OpenSession())
                {
                    id = ((DocumentSession)s).Id;
                    var countCute = s.Query<Dog>().Where(x=>x.Cute).CountLazily();
                    var countNotCute = s.Query<Dog>().Where(x=>x.Cute == false).CountLazily();
                    Assert.Equal(0, countNotCute.Value);
                    Assert.Equal(0, countCute.Value);
                }

                var profiling = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profiling.Requests.Count);
                // multi get

                var getRequests = JsonConvert.DeserializeObject<GetRequest[]>(profiling.Requests[0].PostedData);
                Assert.Equal(2, getRequests.Length);

                Assert.Contains("pageSize=0",getRequests[0].Query);
                Assert.Contains("pageSize=0", getRequests[1].Query);

            }
        }

        [Fact]
        public async Task CanGetCountWithoutGettingAllTheDataLazyAsync()
        {
            using (var store = NewDocumentStore())
            {
                store.InitializeProfiling();
                Guid id;
                
                using (var s = store.OpenAsyncSession())
                {
                    id = ((AsyncDocumentSession)s).Id;
                    var countCute = s.Query<Dog>().Where(x => x.Cute).CountLazilyAsync();
                    var countNotCute = s.Query<Dog>().Where(x => x.Cute == false).CountLazilyAsync();
                    
                    Assert.Equal(0, await countNotCute.Value);
                    Assert.Equal(0, await countCute.Value);
                }

                var profiling = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profiling.Requests.Count);
                // multi get

                var getRequests = JsonConvert.DeserializeObject<GetRequest[]>(profiling.Requests[0].PostedData);
                Assert.Equal(2, getRequests.Length);

                Assert.Contains("pageSize=0", getRequests[0].Query);
                Assert.Contains("pageSize=0", getRequests[1].Query);

            }
        }
    }
}
