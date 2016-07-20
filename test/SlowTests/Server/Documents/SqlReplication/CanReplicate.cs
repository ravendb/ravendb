// -----------------------------------------------------------------------
//  <copyright file="CanReplicate.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.SqlReplication;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client.Document;
using Raven.Database.Util;
using Raven.Server.Documents.SqlReplication;
using Sparrow;
using Xunit;
using Xunit.Sdk;

namespace SlowTests.Server.Documents.SqlReplication
{
    public class CanReplicateSlow : CanReplicate
    {
        [NonLinuxFact]
        public async Task ReplicateMultipleBatches()
        {
            using (var store = await GetDocumentStore())
            {
                CreateRdbmsSchema(store);

                var eventSlim = new ManualResetEventSlim(false);
                var database = await GetDatabase(store.DefaultDatabase);
                int testCount = 5000;
                database.SqlReplicationLoader.AfterReplicationCompleted += statistics =>
                {
                    if (GetOrdersCount(store) == testCount)
                        eventSlim.Set();
                };

                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < testCount; i++)
                    {
                        await bulkInsert.StoreAsync(new Order
                        {
                            OrderLines = new List<OrderLine>
                            {
                                new OrderLine {Cost = 3, Product = "Milk", Quantity = 3},
                                new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                            }

                        });
                    }
                }

                await SetupSqlReplication(store, defaultScript);

                eventSlim.Wait(TimeSpan.FromMinutes(5));

                Assert.Equal(testCount, GetOrdersCount(store));
            }
        }

    }
}
