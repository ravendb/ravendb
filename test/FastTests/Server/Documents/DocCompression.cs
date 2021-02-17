using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using Newtonsoft.Json;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;
using Tests.Infrastructure;
using Voron.Util;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents
{
    public class DocCompression : RavenTestBase
    {
        public DocCompression(ITestOutputHelper output) : base(output)
        {
        }

        public class User
        {
            public string Desc;
            public List<string> Items;
        }

        public static string RandomString(Random random, int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        [LicenseRequiredFact]
        public void Can_compact_from_no_compression_to_compressed()
        {
            var path = NewDataPath();
            if (Directory.Exists(path))
                Directory.Delete(path, true);
            using var store = GetDocumentStore(new Options
            {
                Path = path,
                RunInMemory = false,
            });

            store.Maintenance.Send(new CreateSampleDataOperation());

            var executor = store.GetRequestExecutor();
            using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var cmd = new GetDocumentSize("orders/830-A");
                executor.Execute(cmd, ctx);
                Assert.True(cmd.Result.ActualSize <= cmd.Result.AllocatedSize);
            }

            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
            record.DocumentsCompression = new DocumentsCompressionConfiguration(true, "Orders");
            store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, record.Etag));

            var op = store.Maintenance.Server.Send(new CompactDatabaseOperation(new CompactSettings
            {
                DatabaseName = store.Database,
                Documents = true,
            }));

            op.WaitForCompletion();

            WaitForIndexing(store);

            var operation = store.Maintenance.Send(new GetStatisticsOperation());

            Assert.True(operation.Indexes.All(x => x.State == IndexState.Normal));

            using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var cmd = new GetDocumentSize("orders/830-A");
                executor.Execute(cmd, ctx);
                Assert.True(cmd.Result.ActualSize > cmd.Result.AllocatedSize);
            }
        }

        [LicenseRequiredFact]
        public void Can_compact_from_compression_to_not_compressed()
        {
            var path = NewDataPath();
            if (Directory.Exists(path))
                Directory.Delete(path, true);
            using var store = GetDocumentStore(new Options
            {
                Path = path,
                RunInMemory = false,
                ModifyDatabaseRecord = r => r.DocumentsCompression = new DocumentsCompressionConfiguration(true, "Orders")
        });

            store.Maintenance.Send(new CreateSampleDataOperation());

            var executor = store.GetRequestExecutor();
            using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var cmd = new GetDocumentSize("orders/830-A");
                executor.Execute(cmd, ctx);
                Assert.True(cmd.Result.ActualSize > cmd.Result.AllocatedSize);
            }

            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
            record.DocumentsCompression = new DocumentsCompressionConfiguration(true);
            store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, record.Etag));

            var op = store.Maintenance.Server.Send(new CompactDatabaseOperation(new CompactSettings
            {
                DatabaseName = store.Database,
                Documents = true,
            }));

            op.WaitForCompletion();

            WaitForIndexing(store);

            var operation = store.Maintenance.Send(new GetStatisticsOperation());

            Assert.True(operation.Indexes.All(x => x.State == IndexState.Normal));

            using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var cmd = new GetDocumentSize("orders/830-A");
                executor.Execute(cmd, ctx);
                Assert.True(cmd.Result.ActualSize <= cmd.Result.AllocatedSize);
            }
        }

        [LicenseRequiredFact]
        public void Can_compact_db_with_compressed_collections()
        {
            var path = NewDataPath();
            if(Directory.Exists(path))
                Directory.Delete(path, true);
            using var store = GetDocumentStore(new Options
            {
                Path = path,
                RunInMemory = false,
                ModifyDatabaseRecord = record => record.DocumentsCompression = new DocumentsCompressionConfiguration(true, "Orders")
        });

            store.Maintenance.Send(new CreateSampleDataOperation());

            var op = store.Maintenance.Server.Send(new CompactDatabaseOperation(new CompactSettings{
                DatabaseName = store.Database,
                Documents = true,
            }));

            op.WaitForCompletion();

            var f = Path.GetTempFileName();

            using var _ = new DisposableAction(() => File.Delete(f));

            var operation = store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), f, CancellationToken.None)
                .Result;

            using (var s = store.OpenSession())
            {
                s.Query<Query.Order>().ToList();
            }

            // this verifies that all the data is fine

            operation.WaitForCompletion();
        }

        [LicenseRequiredFact]
        public void Can_write_many_documents_without_breakage()
        {
            var random = new Random(654);
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.DocumentsCompression = new DocumentsCompressionConfiguration(true, "Users")
        });

            var rnd = Enumerable.Range(1, 10)
                .Select(i => RandomString(random, 16))
                .ToList();
            using var s = store.OpenSession();
            for (int i = 0; i < 1024; i++)
            {
                s.Store(new User
                {
                    Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 10)).Select(xi => rnd[xi]))
                }, "users/" + i);
            }
            s.SaveChanges();

        }

        [LicenseRequiredFact]
        public void Can_set_collection_compressed_when_it_has_docs()
        {
            var random = new Random(343);
            using var store = GetDocumentStore();

            var rnd = Enumerable.Range(1, 10)
                .Select(i => new string((char)(65+i), 256))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 0; i < 5; i++)
                {
                    s.Store(new User
                    {
                        Items = Enumerable.Range(1, random.Next(1, 10))
                            .Select(x=>rnd[x])
                            .ToList()
                    }, "users/" + i);
                }

                s.SaveChanges();
            }

            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
            record.DocumentsCompression = new DocumentsCompressionConfiguration(true, "Users");
            store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, record.Etag));

            using (var s = store.OpenSession())
            {
                for (int i = 5; i < 1024; i++)
                {
                    s.Store(new User
                    {
                        Items = Enumerable.Range(1, random.Next(1, 10))
                            .Select(x => rnd[x])
                            .ToList()
                    },"users/" +i);
                }

                s.SaveChanges();
            }

            var executor = store.GetRequestExecutor();
            using var _ = executor.ContextPool.AllocateOperationContext(out var ctx);
            var cmd = new GetDocumentSize("users/1000");
            executor.Execute(cmd, ctx);

            Assert.True(cmd.Result.ActualSize > cmd.Result.AllocatedSize);
        }

        public class DocumentSize
        {
            public int ActualSize { get; set; }
            public int AllocatedSize { get; set; }
            public string DocId { get; set; }
            public string HumaneActualSize { get; set; }
            public string HumaneAllocatedSize { get; set; }
        }

        internal class GetDocumentSize : RavenCommand<DocumentSize>
        {
            private readonly string _id;
            public override bool IsReadRequest => true;

            public GetDocumentSize(string id)
            {
                _id = id;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/docs/size?id={Uri.EscapeDataString(_id)}";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                // quick and dirty for the tests
                Result = JsonConvert.DeserializeObject<DocumentSize>(response.ToString());
            }
        }

        [LicenseRequiredFact]
        public void Can_update_many_documents_without_breakage()
        {
            var random = new Random(654);
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.DocumentsCompression = new DocumentsCompressionConfiguration(true, "Users")
        });

            var rnd = Enumerable.Range(1, 10)
                .Select(i => RandomString(random, 16))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 10)).Select(xi => rnd[xi]))
                    }, "users/" + i);
                }

                s.SaveChanges();
            }

            rnd = Enumerable.Range(1, 64)
                .Select(i => RandomString(random, 512))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 64)).Select(xi => rnd[xi]))
                    }, "users/" + i);
                }

                s.SaveChanges();
            }
        }

        [LicenseRequiredFact]
        public void Can_update_many_documents_without_breakage_to_be_smaller()
        {
            var random = new Random(654);
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.DocumentsCompression = new DocumentsCompressionConfiguration(true, "Users")
        });

            var rnd = Enumerable.Range(1, 10)
                .Select(i => RandomString(random, 16))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 10)).Select(xi => rnd[xi]))
                    }, "users/" + i);
                }

                s.SaveChanges();
            }

            rnd = Enumerable.Range(1, 64)
                .Select(i => RandomString(random, 512))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 256; i < 1024; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 64)).Select(xi => rnd[xi]))
                    }, "users/" + i);
                }

                s.SaveChanges();
            }

            rnd = Enumerable.Range(1, 64)
                .Select(i => RandomString(random, 32))
                .ToList();

            using (var s = store.OpenSession())
            {
                for (int i = 128; i < 768; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 64)).Select(xi => rnd[xi]))
                    }, "users/" + i);
                }

                for (int i = 0; i < 128; i++)
                {
                    s.Store(new User
                    {
                        Desc = string.Join("-", Enumerable.Range(1, random.Next(1, 64)).Select(xi => rnd[xi]))
                    }, "users/" + i+900);
                }

                s.SaveChanges();
            }
        }
    }
}
