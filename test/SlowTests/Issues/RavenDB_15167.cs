using System;
using System.Collections.Generic;
using System.Net.Http;
using FastTests;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15167 : RavenTestBase
    {
        public RavenDB_15167(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Cannot_Setup_Documents_Compression_When_Experimental_Mode_Is_Off()
        {
            DoNotReuseServer(new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability), FeaturesAvailability.Stable.ToString() }
            });

            using (var store = GetDocumentStore())
            {
                var databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                Assert.Null(databaseRecord.DocumentsCompression);

                var databaseName = $"{store.Database}-{Guid.NewGuid()}";
                using (EnsureDatabaseDeletion(databaseName, store))
                {
                    var e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord
                    {
                        DatabaseName = databaseName,
                        DocumentsCompression = new DocumentsCompressionConfiguration
                        {
                            CompressRevisions = true
                        }
                    })));

                    Assert.Contains("Can not use 'Documents Compression'", e.Message);

                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord
                    {
                        DatabaseName = databaseName
                    }));

                    using (var commands = store.Commands(databaseName))
                    {
                        e = Assert.Throws<RavenException>(() => commands.ExecuteJson($"/admin/documents-compression/config?raft-request-id={RaftIdGenerator.NewId()}", HttpMethod.Post, new DocumentsCompressionConfiguration
                        {
                            CompressRevisions = true
                        }));

                        databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(databaseName));
                        Assert.Null(databaseRecord.DocumentsCompression);
                    }
                }
            }
        }
    }
}
