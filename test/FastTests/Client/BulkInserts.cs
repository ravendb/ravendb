using Tests.Infrastructure;
using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class BulkInserts : RavenTestBase
    {
        public BulkInserts(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(false, CompressionLevel.NoCompression, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(false, CompressionLevel.Optimal, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Simple_Bulk_Insert(Options options, bool useSsl, CompressionLevel compressionLevel)
        {
            string dbName = GetDatabaseName();
            X509Certificate2 clientCertificate = null;
            X509Certificate2 adminCertificate = null;
            if (useSsl)
            {
                var certificates = Certificates.SetupServerAuthentication();
                adminCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);
                clientCertificate = Certificates.RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate2.Value, new Dictionary<string, DatabaseAccess>
                {
                    [dbName] = DatabaseAccess.ReadWrite
                });
            }

            options.AdminCertificate = adminCertificate;
            options.ClientCertificate = clientCertificate;
            options.ModifyDatabaseName = s => dbName;

            if (useSsl)
                options.ModifyDocumentStore = s => s.OnFailedRequest += (_, args) => Console.WriteLine($"Failed Request ('{args.Database}'): {args.Url}. Exception: {args.Exception}");

            using (var store = GetDocumentStore(options))
            {
                using (var bulkInsert = store.BulkInsert(new BulkInsertOptions()
                {
                    CompressionLevel = compressionLevel
                }))
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        await bulkInsert.StoreAsync(new FooBar()
                        {
                            Name = "foobar/" + i
                        }, "FooBars/" + i);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var len = session.Advanced.LoadStartingWith<FooBar>("FooBars/", null, 0, 1000, null);
                    Assert.Equal(1000, len.Length);
                }
            }
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Simple_Bulk_Insert_Should_Work(Options options)
        {
            var fooBars = new[]
            {
                new FooBar
                {
                    Name = "John Doe"
                },
                new FooBar
                {
                    Name = "Jane Doe"
                },
                new FooBar
                {
                    Name = "Mega John"
                },
                new FooBar
                {
                    Name = "Mega Jane"
                }
            };
            using (var store = GetDocumentStore(options))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    await bulkInsert.StoreAsync(fooBars[0]);
                    await bulkInsert.StoreAsync(fooBars[1]);
                    await bulkInsert.StoreAsync(fooBars[2]);
                    await bulkInsert.StoreAsync(fooBars[3]);
                }

                store.GetRequestExecutor(store.Database).ContextPool.AllocateOperationContext(out JsonOperationContext context);

                var getDocumentCommand = new GetDocumentsCommand(new[] { "FooBars/1-A", "FooBars/2-A", "FooBars/3-A", "FooBars/4-A" }, includes: null, metadataOnly: false);

                store.GetRequestExecutor(store.Database).Execute(getDocumentCommand, context);

                var results = getDocumentCommand.Result.Results;

                Assert.Equal(4, results.Length);

                var doc1 = results[0];
                var doc2 = results[1];
                var doc3 = results[2];
                var doc4 = results[3];
                Assert.NotNull(doc1);
                Assert.NotNull(doc2);
                Assert.NotNull(doc3);
                Assert.NotNull(doc4);

                object name;
                ((BlittableJsonReaderObject)doc1).TryGetMember("Name", out name);
                Assert.Equal("John Doe", name.ToString());
                ((BlittableJsonReaderObject)doc2).TryGetMember("Name", out name);
                Assert.Equal("Jane Doe", name.ToString());
                ((BlittableJsonReaderObject)doc3).TryGetMember("Name", out name);
                Assert.Equal("Mega John", name.ToString());
                ((BlittableJsonReaderObject)doc4).TryGetMember("Name", out name);
                Assert.Equal("Mega Jane", name.ToString());
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
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

                    var e = await Assert.ThrowsAsync<InvalidOperationException>(async () => { await Parallel.ForEachAsync(localList, async (element, _) => { await bulkInsert.StoreAsync(element); }); });

                    Assert.Contains("Bulk Insert store methods cannot be executed concurrently", e.Message);
                }
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public void CanUseCustomSerializer()
        {
            IJsonSerializer jsonSerializer = null;
            RequestExecutor requestExecutor = null;
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = store => store.Conventions.BulkInsert.TrySerializeEntityToJsonStream = (entity, metadata, streamWriter) =>
                {
                    requestExecutor ??= store.GetRequestExecutor(store.Database);
                    requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context);
                    using (var json = store.Conventions.Serialization.DefaultConverter.ToBlittable(entity, metadata, context, jsonSerializer ??= store.Conventions.Serialization.CreateSerializer()))
                    {
                        json._context.Sync.Write(streamWriter.BaseStream, json);
                    }

                    return true;
                }
            }))
            {
                string userId1;
                string userId2;
                using (var bulkInsert = store.BulkInsert())
                {
                    var user1 = new User { Name = "Grisha1" };
                    bulkInsert.Store(user1);
                    userId1 = user1.Id;

                    var user2 = new User { Name = "Grisha2" };
                    bulkInsert.Store(user2);
                    userId2 = user2.Id;
                }

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>(userId1);
                    Assert.NotNull(user1);
                    Assert.Equal("Grisha1", user1.Name);

                    var user2 = session.Load<User>(userId2);
                    Assert.NotNull(user2);
                    Assert.Equal("Grisha2", user2.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public void CanUseNewtonsoftSerializer()
        {
            JsonSerializer jsonSerializer = null;

            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = store => store.Conventions.BulkInsert.TrySerializeEntityToJsonStream = (entity, metadata, streamWriter) =>
                {
                    JObject jo = JObject.FromObject(entity);

                    var metadataJObject = new JObject();
                    foreach (var keyValue in metadata)
                    {
                        metadataJObject.Add(keyValue.Key, new JValue(keyValue.Value));
                    }

                    jo[Raven.Client.Constants.Documents.Metadata.Key] = metadataJObject;

                    var jsonWriter = new JsonTextWriter(streamWriter);
                    jsonSerializer ??= new JsonSerializer();
                    jsonSerializer.Serialize(jsonWriter, jo);
                    jsonWriter.Flush();

                    return true;
                }
            }))
            {
                string userId1;
                string userId2;
                using (var bulkInsert = store.BulkInsert())
                {
                    var user1 = new User { Name = "Grisha1" };
                    bulkInsert.Store(user1);
                    userId1 = user1.Id;

                    var user2 = new User { Name = "Grisha2" };
                    bulkInsert.Store(user2);
                    userId2 = user2.Id;
                }

                using (var session = store.OpenSession())
                {
                    var user1 = session.Load<User>(userId1);
                    Assert.NotNull(user1);
                    Assert.Equal("Grisha1", user1.Name);

                    var user2 = session.Load<User>(userId2);
                    Assert.NotNull(user2);
                    Assert.Equal("Grisha2", user2.Name);
                }
            }
        }

        private class FooBarIndex : AbstractIndexCreationTask<FooBar>
        {
            public FooBarIndex()
            {
                Map = foos => foos.Select(x => new
                {
                    x.Name
                });
            }
        }

        private class FooBar : IEquatable<FooBar>
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
