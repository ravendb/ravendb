using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace FastTests.Client
{
    public class BulkInserts : RavenTestBase
    {
        [Theory]
        [InlineData(false)]
        public async Task Simple_Bulk_Insert(bool useSsl)
        {
            string dbName = GetDatabaseName();
            X509Certificate2 clientCertificate = null;
            X509Certificate2 adminCertificate = null;
            if (useSsl)
            {
                var serverCertPath = SetupServerAuthentication();
                adminCertificate = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
                clientCertificate = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>
                {
                    [dbName] = DatabaseAccess.ReadWrite
                });
            }

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCertificate,
                ClientCertificate = clientCertificate,
                ModifyDatabaseName = s => dbName
            }))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        await bulkInsert.StoreAsync(new FooBar() { Name = "foobar/" + i }, "FooBars/" + i);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var len = session.Advanced.LoadStartingWith<FooBar>("FooBars/", null, 0, 1000, null);
                    Assert.Equal(1000, len.Length);
                }
            }
        }

        [Fact]
        public async Task Bulk_Insert_Should_Throw_On_StoreAsync_Concurrent_Calls()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    var localList = new User[32];
                    for (var index = 0; index < localList.Length; index++)
                    {
                        localList[index] = new User
                        {
                            Id = "myTest/"
                        };
                    }

                    // Assert catching cocuurency exception using string
                    string msg = null;

                    var result = Parallel.ForEach(localList, async element =>
                    {
                        var localElement = element;
                        try
                        {
                            await bulkInsert.StoreAsync(localElement).ConfigureAwait(false);
                        }
                        catch (ConcurrencyException e)
                        {
                            if (msg == null)
                                msg = e.Message;
                        }
                    });

                    SpinWait.SpinUntil(() => result.IsCompleted, TimeSpan.FromSeconds(30));
                    Assert.True(result.IsCompleted);
                    Assert.Contains("Store/StoreAsync in bulkInsert concurrently is forbidden", msg);
                }
            }
        }

        public class FooBarIndex : AbstractIndexCreationTask<FooBar>
        {
            public FooBarIndex()
            {
                Map = foos => foos.Select(x => new { x.Name });
            }
        }

        public class FooBar : IEquatable<FooBar>
        {
            public string Name { get; set; }

            public bool Equals(FooBar other)
            {
                if (ReferenceEquals(null, other))
                    return false;
                if (ReferenceEquals(this, other))
                    return true;
                return string.Equals(Name, other.Name);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (ReferenceEquals(this, obj))
                    return true;
                if (obj.GetType() != this.GetType())
                    return false;
                return Equals((FooBar)obj);
            }

            public override int GetHashCode()
            {
                return Name?.GetHashCode() ?? 0;
            }

            public static bool operator ==(FooBar left, FooBar right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(FooBar left, FooBar right)
            {
                return !Equals(left, right);
            }
        }
    }
}
