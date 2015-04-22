// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2514.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Database.Actions;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Raven.Tests.MailingList;

using Xunit;

using Raven.Abstractions.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_2514 : RavenTestBase
    {
        [Fact]
        public void CanBulkInsert()
        {
            const int bulkInsertSize = 2000;
            using (var store = NewRemoteDocumentStore())
            {
                // we don't use using statement here becase dispose would throw OperationCanceledException and we want to assert this
                var bulkInsert = store.BulkInsert(options: new BulkInsertOptions { BatchSize = 1 });

                Task.Factory.StartNew(() =>
                {
                    // first and kill first operation
                    while (true)
                    {
                        var response = (RavenJArray)store.JsonRequestFactory.CreateHttpJsonRequest(
                            new CreateHttpJsonRequestParams(null, store.Url.ForDatabase(store.DefaultDatabase) + "/operations", HttpMethods.Get,
                                store.DatabaseCommands.PrimaryCredentials, store.Conventions)).ReadResponseJson();
                        var taskList = response.Select(
                            t => ((RavenJObject)t).Deserialize<TaskActions.PendingTaskDescriptionAndStatus>(store.Conventions)).ToList();
                        if (taskList.Count > 0)
                        {
                            var operationId = taskList.First().Id;
                            store.JsonRequestFactory.CreateHttpJsonRequest(
                                new CreateHttpJsonRequestParams(null, store.Url.ForDatabase(store.DefaultDatabase) + "/operation/kill?id=" + operationId,
                                    HttpMethods.Get, store.DatabaseCommands.PrimaryCredentials, store.Conventions)).ExecuteRequest();
                            break;
                        }
                    }
                });


                ExpectAggregateOrOperationCanceledException(() =>
                {
                    for (var i = 0; i < bulkInsertSize; i++)
                    {
                        bulkInsert.Store(new FailingBulkInsertTest.SampleData { Name = "New Data" + i });
                        Thread.Sleep(30);
                    }
                });

                ExpectAggregateOrOperationCanceledException(bulkInsert.Dispose);
            }
        }

        void ExpectAggregateOrOperationCanceledException(Action action)
        {
            try
            {
                action();
                Assert.True(false);
            }
            catch (AggregateException e)
            {
                Assert.True(e.ExtractSingleInnerException() is OperationCanceledException);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}