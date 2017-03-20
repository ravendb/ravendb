// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2514.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Server.Commands;
using Raven.Server.Extensions;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2514 : RavenTestBase
    {
        [Fact(Skip = "RavenDB-6498")]
        public void CanKillBulkInsert()
        {
            const int bulkInsertSize = 2000;
            using (var store = GetDocumentStore())
            {
                // we don't use using statement here becase dispose would throw OperationCanceledException and we want to assert this
                var bulkInsert = store.BulkInsert();

                bulkInsert.Store(new User { Name = "New Data" });

                var requestExecutor = store.GetRequestExecuter();

                JsonOperationContext context;
                using (requestExecutor.ContextPool.AllocateOperationContext(out context))
                {
                    var killCommand = new CloseTcpConnectionCommand(1);

                    requestExecutor.Execute(killCommand, context);

                    ExpectEndOfStreamOrOperationCanceledException(() =>
                    {
                        for (var i = 0; i < bulkInsertSize; i++)
                        {
                            bulkInsert.Store(new User { Name = "New Data" + i });
                            Thread.Sleep(30);
                        }
                    });

                    ExpectEndOfStreamOrOperationCanceledException(bulkInsert.Dispose);
                }
            }
        }

        private static void ExpectEndOfStreamOrOperationCanceledException(Action action)
        {
            try
            {
                action();
                Assert.True(false);
            }
            catch (EndOfStreamException)
            {
            }
            catch (OperationCanceledException)
            {
            }
            catch (IOException)
            {
            }
        }
    }
}
