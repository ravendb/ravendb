using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Graph;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Security;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18496 : ReplicationTestBase
    {
        public RavenDB_18496(ITestOutputHelper output) : base(output)
        {
        }
        

        [Fact]
        public async Task DeletingMasterKeyForExistedEncryptedDatabaseShouldFail_2()
        {
            EncryptedServer(out var certificates, out var databaseName);

            using (var encryptedStore = GetDocumentStore(new Options
                   {
                       ModifyDatabaseName = _ => databaseName,
                       ClientCertificate = certificates.ServerCertificate.Value,
                       AdminCertificate = certificates.ServerCertificate.Value,
                       Encrypted = true
                   }))
            using (var store2 = GetDocumentStore(new Options
                   {
                       ClientCertificate = certificates.ServerCertificate.Value
                   }))
            {
                var db = await GetDocumentDatabaseInstanceFor(encryptedStore);
                db.Configuration.Replication.MaxSizeToSend = new Size(16, SizeUnit.Kilobytes);

                await SetupReplicationAsync(encryptedStore, store2);

                await EnsureReplicatingAsync(encryptedStore, store2);

                await EnsureNoReplicationLoop(Server, databaseName);
            }
        }
    }
}
