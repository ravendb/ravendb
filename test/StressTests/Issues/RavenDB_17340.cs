﻿using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_17340 : RavenTestBase
    {
        public RavenDB_17340(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformTheory(RavenTestCategory.BulkInsert, RavenArchitecture.AllX64)]
        [InlineData(6)]
        [InlineData(100)]
        [InlineData(600)]
        public async Task SendingDocumentWithLargeFieldInBulkInsert(int sizeInMb)
        {
            var mb = 1024 * 1024;
            var objSize = sizeInMb * mb;
            var bigObj = new BigFieldObject()
            {
                textField = new string('*', objSize)
            };

            using (var store = GetDocumentStore())
            {
                await using (var bulkInsert = store.BulkInsert())
                {
                    await bulkInsert.StoreAsync(new BigFieldObject());
                    await bulkInsert.StoreAsync(new BigFieldObject());
                    await bulkInsert.StoreAsync(new BigFieldObject());
                    await bulkInsert.StoreAsync(bigObj);
                }
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.BulkInsert, RavenArchitecture.AllX86)]
        [InlineData(6)]
        [InlineData(10)]
        public async Task SendingDocumentWithLargeFieldInBulkInsert_32bit(int sizeInMb)
        {
            await SendingDocumentWithLargeFieldInBulkInsert(sizeInMb);
        }

        public class BigFieldObject
        {
            public string textField;
        }

    }
}
