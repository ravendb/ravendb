using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents
{
    public class CompressAllCollectionsTest : RavenTestBase
    {
        public CompressAllCollectionsTest(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CompressAllCollectionsAfterDocsChange()
        {
            var dbname = "CompressAllCollectionsDB";
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = (name) => dbname,
                RunInMemory = false
            });

            using (var session = store.OpenAsyncSession())
            {
                var arr = new string[100000];
                for (int i = 0; i < arr.Length; i++)
                {
                    arr[i] = "stv";
                }

                var arr2 = new string[100000];
                for (int i = 0; i < arr2.Length; i++)
                {
                    arr2[i] = "val";
                }

                var user = new User { arr = arr };
                var company = new Company { arr = arr2 };

                await session.StoreAsync(user, "users/1");
                await session.StoreAsync(company, "companies/1");

                await session.SaveChangesAsync();
            }

            var executor = store.GetRequestExecutor();
            long originalCompanySize, originalUserSize;
            using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var companySize = new DocCompression.GetDocumentSize("companies/1");
                await executor.ExecuteAsync(companySize, ctx);
                originalCompanySize = companySize.Result.AllocatedSize;

                var userSize = new DocCompression.GetDocumentSize("users/1");
                await executor.ExecuteAsync(userSize, ctx);
                originalUserSize = userSize.Result.AllocatedSize;
            }

            //turn on compression
            store.Maintenance.Send(new UpdateDocumentsCompressionConfigurationOperation(new DocumentsCompressionConfiguration(false, true)));

            //make a change to docs so they'll be rewritten and compressed
            using (var session = store.OpenAsyncSession())
            {
                Company comp = await session.LoadAsync<Company>("companies/1");
                comp.arr[0] = "str";

                User user = await session.LoadAsync<User>("users/1");
                user.arr[0] = "vaz";

                await session.SaveChangesAsync();
            }
            
            //get sizes after rewrite
            long compressedUserSize, compressedCompanySize;
            using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var companySize = new DocCompression.GetDocumentSize("companies/1");
                await executor.ExecuteAsync(companySize, ctx);
                compressedCompanySize = companySize.Result.AllocatedSize;

                var userSize = new DocCompression.GetDocumentSize("users/1");
                await executor.ExecuteAsync(userSize, ctx);
                compressedUserSize = userSize.Result.AllocatedSize;
            }

            //check both docs (both collections) have been compressed
            Assert.True(originalCompanySize * 0.25 > compressedCompanySize);
            Assert.True(originalUserSize * 0.25 > compressedUserSize);
        }

        [Fact]
        public async Task CompressAllCollectionsAfterCompactDatabaseCalled()
        {
            var dbname = "CompressAllCollectionsDB";
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = (name) => dbname,
                RunInMemory = false
            });
            
            using (var session = store.OpenAsyncSession())
            {
                var arr = new string[100000];
                for (int i = 0; i < arr.Length; i++)
                {
                    arr[i] = "stv";
                }

                var arr2 = new string[100000];
                for (int i = 0; i < arr2.Length; i++)
                {
                    arr2[i] = "val";
                }

                var user = new User {arr = arr};
                var company = new Company { arr = arr2 };

                await session.StoreAsync(user, "users/1");
                await session.StoreAsync(company, "companies/1");

                await session.SaveChangesAsync();
            }
            
            var executor = store.GetRequestExecutor();
            long originalCompanySize, originalUserSize;
            using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var companySize = new DocCompression.GetDocumentSize("companies/1");
                await executor.ExecuteAsync(companySize, ctx);
                originalCompanySize = companySize.Result.AllocatedSize;

                var userSize = new DocCompression.GetDocumentSize("users/1");
                await executor.ExecuteAsync(userSize, ctx);
                originalUserSize = userSize.Result.AllocatedSize;
            }

            //turn on compression
            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
            record.DocumentsCompression = new DocumentsCompressionConfiguration(false, true);
            store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, record.Etag));

            //run compact database
            CompactSettings settings = new CompactSettings
            {
                DatabaseName = dbname,
                Documents = true
            };
            var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(settings));
            await operation.WaitForCompletionAsync();

            long compressedUserSize, compressedCompanySize;
            using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var companySize = new DocCompression.GetDocumentSize("companies/1");
                await executor.ExecuteAsync(companySize, ctx);
                compressedCompanySize = companySize.Result.AllocatedSize;

                var userSize = new DocCompression.GetDocumentSize("users/1");
                await executor.ExecuteAsync(userSize, ctx);
                compressedUserSize = userSize.Result.AllocatedSize;
            }

            Assert.True(originalCompanySize * 0.25 > compressedCompanySize);
            Assert.True(originalUserSize * 0.25 > compressedUserSize);
        }

        [Fact]
        public async Task SetupCompressAllCollectionsBeforeDocsAdded()
        {
            var dbname = "CompressAllCollectionsDB";
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = (name) => dbname,
                RunInMemory = false,
                ModifyDatabaseRecord = (record) =>
                {
                    record.DocumentsCompression = new DocumentsCompressionConfiguration(false, true);
                }
            });

            using (var session = store.OpenAsyncSession())
            {
                var arr = new string[100000];
                for (int i = 0; i < arr.Length; i++)
                {
                    arr[i] = "stv";
                }

                var arr2 = new string[100000];
                for (int i = 0; i < arr2.Length; i++)
                {
                    arr2[i] = "val";
                }

                var user = new User { arr = arr };
                var company = new Company { arr = arr2 };

                await session.StoreAsync(user, "users/1");
                await session.StoreAsync(company, "companies/1");

                await session.SaveChangesAsync();
            }

            var dbrecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
            Assert.True(dbrecord.DocumentsCompression.CompressAllCollections);

            var executor = store.GetRequestExecutor();
            using (var _ = executor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var companySize = new DocCompression.GetDocumentSize("companies/1");
                await executor.ExecuteAsync(companySize, ctx);
                Assert.True(companySize.Result.ActualSize * 0.25 > companySize.Result.AllocatedSize);

                var userSize = new DocCompression.GetDocumentSize("users/1");
                await executor.ExecuteAsync(userSize, ctx);
                Assert.True(userSize.Result.ActualSize * 0.25 > userSize.Result.AllocatedSize);
            }
        }
        
        private class Company
        {
            public string[] arr;
        }
        private class User
        {
            public string[] arr;
        }
    }
    
}
