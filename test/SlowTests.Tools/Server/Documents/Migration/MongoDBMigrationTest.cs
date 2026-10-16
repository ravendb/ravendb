using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace SlowTests.Tools.Server.Documents.Migration
{
    public class MongoDBMigrationTest : RavenTestBase
    {
        public MongoDBMigrationTest(ITestOutputHelper output) : base(output)
        {
        }

        private class Book
        {
            public class Comparer : IEqualityComparer<Book>
            {
                public bool Equals(Book x, Book y) => x.Title == y.Title;
                public int GetHashCode(Book obj) => throw new NotImplementedException();
            }

            public string Title { get; set; }
        }

        private class Movie
        {
            public string Title { get; set; }

            public class Comparer : IEqualityComparer<Movie>
            {
                public bool Equals(Movie x, Movie y) => x.Title == y.Title;
                public int GetHashCode(Movie obj) => throw new NotImplementedException();
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport, Requires = RavenServiceRequirement.MongoDB)]
        public async Task MigrateMongodb_WhenHasTwoCollectionAndImportAll_ShouldImportTheTwoCollectionWithAllThereDocuments()
        {
            var expectedBooks = new[] { new Book { Title = "Great Book1!!!" } };
            var expectedMovies = new[] { new Movie { Title = "Great Movie1!!!" } };

            var databaseName = $"Test{Guid.NewGuid().ToString()}";

            var connectionString = MongoDBConnectionString.Instance.ConnectionString.Value;
            var client = new MongoClient(connectionString);
            var db = client.GetDatabase(databaseName);

            try
            {
                var booksCollection = db.GetCollection<Book>("Books");
                await booksCollection.InsertManyAsync(expectedBooks);

                var moviesCollection = db.GetCollection<Movie>("Movies");
                await moviesCollection.InsertManyAsync(expectedMovies);


                using (var store = GetDocumentStore())
                {
                    await MigrateFromMongoDB(store, connectionString, databaseName, migrateGridFS: false);

                    using (var session = store.OpenAsyncSession())
                    {
                        var actualBooks = await session.Query<Book>().ToListAsync();
                        var actualMovies = await session.Query<Movie>().ToListAsync();

                        AssertEquivalent(expectedBooks, actualBooks, new Book.Comparer());
                        AssertEquivalent(expectedMovies, actualMovies, new Movie.Comparer());
                    }
                }
            }
            finally
            {
                client.DropDatabase(databaseName);
            }
        }

        private class GuidItem
        {
            public string Id { get; set; }
            public Guid Standard { get; set; }
            public Guid Legacy { get; set; }
        }

        private class GridFSFile
        {
            public string Owner { get; set; }
        }

        [RavenFact(RavenTestCategory.BackupExportImport, Requires = RavenServiceRequirement.MongoDB)]
        public async Task MigrateMongodb_ShouldMigrateGuidsAndGridFSFiles()
        {
            var guid = Guid.Parse("01020304-0506-0708-090a-0b0c0d0e0f10");
            var owner = ObjectId.GenerateNewId();

            var databaseName = $"Test{Guid.NewGuid()}";

            var connectionString = MongoDBConnectionString.Instance.ConnectionString.Value;
            var client = new MongoClient(connectionString);
            var db = client.GetDatabase(databaseName);

            try
            {
                // both the standard (subtype 4) and the legacy C# (subtype 3) UUID representations
                await db.GetCollection<BsonDocument>("GuidItems").InsertManyAsync(new[]
                {
                    new BsonDocument
                    {
                        { "_id", "guidItems/1" },
                        { "Standard", new BsonBinaryData(guid, GuidRepresentation.Standard) },
                        { "Legacy", new BsonBinaryData(guid, GuidRepresentation.CSharpLegacy) }
                    },
                    new BsonDocument
                    {
                        { "_id", new BsonBinaryData(guid, GuidRepresentation.CSharpLegacy) },
                        { "Standard", new BsonBinaryData(guid, GuidRepresentation.Standard) }
                    }
                });

                var bucket = new GridFSBucket(db);
                var fileWithContentTypeInMetadata = await bucket.UploadFromBytesAsync("a.txt", Encoding.UTF8.GetBytes("first file"),
                    new GridFSUploadOptions { Metadata = new BsonDocument { { "contentType", "text/plain" }, { "Owner", owner } } });
                var fileWithTopLevelContentType = await bucket.UploadFromBytesAsync("b.csv", Encoding.UTF8.GetBytes("second file"),
                    new GridFSUploadOptions { Metadata = new BsonDocument { { "Owner", owner } } });

                // the deprecated top-level 'contentType' field written by older drivers
                await db.GetCollection<BsonDocument>("fs.files").UpdateOneAsync(
                    new BsonDocument("_id", fileWithTopLevelContentType),
                    new BsonDocument("$set", new BsonDocument("contentType", "text/csv")));

                using (var store = GetDocumentStore())
                {
                    await MigrateFromMongoDB(store, connectionString, databaseName, migrateGridFS: true);

                    using (var session = store.OpenAsyncSession())
                    {
                        var item1 = await session.LoadAsync<GuidItem>("guidItems/1");
                        Assert.NotNull(item1);
                        Assert.Equal(guid, item1.Standard);
                        Assert.Equal(guid, item1.Legacy);

                        var item2 = await session.LoadAsync<GuidItem>(guid.ToString());
                        Assert.NotNull(item2);
                        Assert.Equal(guid, item2.Standard);

                        await AssertGridFSFile(session, fileWithContentTypeInMetadata.ToString(), owner, "text/plain", "first file");
                        await AssertGridFSFile(session, fileWithTopLevelContentType.ToString(), owner, "text/csv", "second file");
                    }
                }
            }
            finally
            {
                client.DropDatabase(databaseName);
            }
        }

        private static async Task AssertGridFSFile(IAsyncDocumentSession session, string id, ObjectId expectedOwner, string expectedContentType, string expectedContent)
        {
            var file = await session.LoadAsync<GridFSFile>(id);
            Assert.NotNull(file);
            Assert.Equal(expectedOwner.ToString(), file.Owner);

            var attachmentNames = session.Advanced.Attachments.GetNames(file);
            var attachmentName = Assert.Single(attachmentNames);
            Assert.Equal(expectedContentType, attachmentName.ContentType);

            using (var attachment = await session.Advanced.Attachments.GetAsync(file, attachmentName.Name))
            using (var reader = new StreamReader(attachment.Stream))
            {
                Assert.Equal(expectedContent, await reader.ReadToEndAsync());
            }
        }

        private async Task MigrateFromMongoDB(IDocumentStore store, string connectionString, string databaseName, bool migrateGridFS)
        {
            var path = NewDataPath();
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }

            Directory.CreateDirectory(path);

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var inputConfiguration = new DynamicJsonValue
                {
                    ["DatabaseName"] = databaseName,
                    ["Command"] = "export",
                    ["ExportFilePath"] = path,
                    ["ConnectionString"] = connectionString,
                    ["MigrateGridFS"] = migrateGridFS
                };
                var inputConfigurationStr = context.ReadObject(inputConfiguration, "InputConfiguration").ToString();

                Raven.Migrator.Program.Main(new[] { "mongodb", "-j", inputConfigurationStr });
            }

            var databaseDumpFilePath = Directory.GetFiles(path).FirstOrDefault();
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), databaseDumpFilePath);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
        }

        private static void AssertEquivalent<T>(IEnumerable<T> expected, IEnumerable<T> actual, IEqualityComparer<T> comparer)
        {
            Assert.Equal(expected.Count(), actual.Count());

            void Action(T b1) => Assert.Contains(b1, expected, comparer);

            var actions = expected.Select(b => (Action<T>)Action).ToArray();

            Assert.Collection(actual, actions);
        }
    }
}
