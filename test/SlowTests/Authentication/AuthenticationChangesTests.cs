using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Changes;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Authentication
{
    public class AuthenticationChangesTests : RavenTestBase
    {
        public AuthenticationChangesTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ChangesApi | RavenTestCategory.Certificates)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task ChangesWithAuthentication(Options options, bool with2Eku)
        {
            var certificates = Certificates.SetupServerAuthentication(with2Eku: with2Eku);
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
            var userCert = Certificates.RegisterClientCertificate(certificates.ServerCertificateForCommunication.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
            {
                [dbName] = DatabaseAccess.ReadWrite
            });

            options.AdminCertificate = adminCert;
            options.ClientCertificate = userCert;
            options.ModifyDatabaseName = s => dbName;

            using (var store = GetDocumentStore(options))
            {
                var list = new BlockingCollection<DocumentChange>();
                var taskObservable = store.Changes();
                await taskObservable.EnsureConnectedNow();
                var observableWithTask = taskObservable.ForDocument("users/1");

                observableWithTask.Subscribe(list.Add);
                await observableWithTask.EnsureSubscribedNow();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(list.TryTake(out var documentChange, TimeSpan.FromSeconds(1)));

                Assert.Equal("users/1", documentChange.Id);
                Assert.Equal(documentChange.Type, DocumentChangeTypes.Put);
                Assert.NotNull(documentChange.ChangeVector);
            }
        }
    }
}
