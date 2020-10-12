// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2514.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using FastTests;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.BulkInsert;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2514 : RavenTestBase
    {
        public RavenDB_2514(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanKillBulkInsert()
        {
            const int bulkInsertSize = 2000;
            using (var store = GetDocumentStore())
            {
                // we don't use using statement here becase dispose would throw OperationCanceledException and we want to assert this
                var bulkInsert = store.BulkInsert();

                for (var i = 0; i < bulkInsertSize; i++)
                {
                    bulkInsert.Store(new User { Name = "New Data" });
                }
                
                bulkInsert.Abort();

                ExpectedErrorOnRequest(() =>
                {
                    for (var i = 0; i < bulkInsertSize; i++)
                    {
                        bulkInsert.Store(new User { Name = "New Data" + i });
                        Thread.Sleep(30);
                    }
                });

                ExpectedErrorOnRequest(bulkInsert.Dispose);
            
            }
        }

        private static void ExpectedErrorOnRequest(Action action)
        {
            try
            {
                action();
                Assert.True(false);
            }
            catch(BulkInsertAbortedException) { }
            catch (IOException) { }
            catch (HttpRequestException) { }
            catch (RequestedNodeUnavailableException) {}
        }
    }
}
