using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.Json;
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

        private static readonly Guid TestGuid = Guid.Parse("01020304-0506-0708-090a-0b0c0d0e0f10");

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

            using (var mongo = new MongoTestDatabase())
            {
                var booksCollection = mongo.Database.GetCollection<Book>("Books");
                await booksCollection.InsertManyAsync(expectedBooks);

                var moviesCollection = mongo.Database.GetCollection<Movie>("Movies");
                await moviesCollection.InsertManyAsync(expectedMovies);

                using (var store = GetDocumentStore())
                {
                    await MigrateFromMongoDB(store, mongo);

                    using (var session = store.OpenAsyncSession())
                    {
                        var actualBooks = await session.Query<Book>().ToListAsync();
                        var actualMovies = await session.Query<Movie>().ToListAsync();

                        AssertEquivalent(expectedBooks, actualBooks, new Book.Comparer());
                        AssertEquivalent(expectedMovies, actualMovies, new Movie.Comparer());
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport, Requires = RavenServiceRequirement.MongoDB)]
        public async Task MigrateMongodb_ShouldConvertAllBsonTypes()
        {
            var objectId = ObjectId.GenerateNewId();

            using (var mongo = new MongoTestDatabase())
            {
                await mongo.Database.GetCollection<BsonDocument>("Items").InsertOneAsync(new BsonDocument
                {
                    { "_id", "item" },
                    { "String", "text" },
                    { "Int32", 42 },
                    { "Int64", 1234567890123L },
                    { "Double", 1.5 },
                    { "Decimal", new Decimal128(12.34m) },
                    { "DecimalWithMorePrecisionThanDecimal", Decimal128.Parse("0.1234567890123456789012345678901234") },
                    { "DecimalOutOfDecimalRange", Decimal128.Parse("1E+100") },
                    { "DecimalNaN", Decimal128.QNaN },
                    { "Boolean", true },
                    { "ObjectId", objectId },
                    { "Date", new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc) },
                    { "Null", BsonNull.Value },
                    { "Undefined", BsonUndefined.Value },
                    { "StandardGuid", new BsonBinaryData(TestGuid, GuidRepresentation.Standard) },
                    { "LegacyGuid", new BsonBinaryData(TestGuid, GuidRepresentation.CSharpLegacy) },
                    { "Binary", new BsonBinaryData(new byte[] { 1, 2, 3 }) },
                    { "Regex", new BsonRegularExpression("abc", "i") },
                    { "Code", new BsonJavaScript("function() { return 1; }") },
                    { "Symbol", BsonSymbolTable.Lookup("sym") },
                    { "Timestamp", new BsonTimestamp(1, 2) },
                    { "MinKey", BsonMinKey.Value },
                    { "MaxKey", BsonMaxKey.Value },
                    { "CodeWithScope", new BsonJavaScriptWithScope("function() { return x; }", new BsonDocument("x", 1)) },
                    { "OutOfRangeDate", new BsonDateTime(long.MaxValue) },
                    { "Nested", new BsonDocument("Inner", new BsonDocument("Value", 1)) },
                    { "Array", new BsonArray { 1, "two", new BsonDocument("Three", 3) } },
                    { "@metadata", new BsonDocument("Mine", true) }
                });

                using (var store = GetDocumentStore())
                {
                    await MigrateFromMongoDB(store, mongo);

                    using (var commands = store.Commands())
                    {
                        var document = await commands.GetAsync("Items/item");
                        Assert.NotNull(document);
                        var json = document.BlittableJson;

                        Assert.Equal("text", Get<string>(json, "String"));
                        Assert.Equal(42, Get<long>(json, "Int32"));
                        Assert.Equal(1234567890123L, Get<long>(json, "Int64"));
                        Assert.Equal(1.5, Get<double>(json, "Double"));
                        Assert.Equal(12.34m, Get<decimal>(json, "Decimal"));
                        // values that a decimal can't hold exactly are kept as strings
                        Assert.Equal("0.1234567890123456789012345678901234", Get<string>(json, "DecimalWithMorePrecisionThanDecimal"));
                        Assert.Equal("1E+100", Get<string>(json, "DecimalOutOfDecimalRange"));
                        Assert.Equal("NaN", Get<string>(json, "DecimalNaN"));
                        Assert.True(Get<bool>(json, "Boolean"));
                        Assert.Equal(objectId.ToString(), Get<string>(json, "ObjectId"));
                        Assert.Equal("2020-01-02T03:04:05Z", Get<string>(json, "Date"));
                        Assert.Null(Get<object>(json, "Null"));
                        Assert.Null(Get<object>(json, "Undefined"));
                        Assert.Equal(TestGuid.ToString(), Get<string>(json, "StandardGuid"));
                        Assert.Equal(TestGuid.ToString(), Get<string>(json, "LegacyGuid"));
                        Assert.Equal("AQID", Get<string>(json, "Binary"));
                        Assert.Equal("/abc/i", Get<string>(json, "Regex"));
                        Assert.Equal("function() { return 1; }", Get<string>(json, "Code"));
                        Assert.Equal("sym", Get<string>(json, "Symbol"));

                        var timestamp = Get<BlittableJsonReaderObject>(Get<BlittableJsonReaderObject>(json, "Timestamp"), "$timestamp");
                        Assert.Equal(1, Get<long>(timestamp, "t"));
                        Assert.Equal(2, Get<long>(timestamp, "i"));
                        Assert.Equal(1, Get<long>(Get<BlittableJsonReaderObject>(json, "MinKey"), "$minKey"));
                        Assert.Equal(1, Get<long>(Get<BlittableJsonReaderObject>(json, "MaxKey"), "$maxKey"));

                        var codeWithScope = Get<BlittableJsonReaderObject>(json, "CodeWithScope");
                        Assert.Equal("function() { return x; }", Get<string>(codeWithScope, "$code"));
                        Assert.Equal(1, Get<long>(Get<BlittableJsonReaderObject>(codeWithScope, "$scope"), "x"));

                        var outOfRangeDate = Get<BlittableJsonReaderObject>(Get<BlittableJsonReaderObject>(json, "OutOfRangeDate"), "$date");
                        Assert.Equal(long.MaxValue.ToString(), Get<string>(outOfRangeDate, "$numberLong"));

                        Assert.Equal(1, Get<long>(Get<BlittableJsonReaderObject>(Get<BlittableJsonReaderObject>(json, "Nested"), "Inner"), "Value"));

                        var array = Get<BlittableJsonReaderArray>(json, "Array");
                        Assert.Equal(3, array.Length);
                        Assert.Equal(1L, array[0]);
                        Assert.Equal("two", array[1].ToString());
                        Assert.Equal(3, Get<long>((BlittableJsonReaderObject)array[2], "Three"));

                        // a MongoDB '@metadata' property can't overwrite the RavenDB metadata
                        Assert.True(Get<bool>(Get<BlittableJsonReaderObject>(json, "@metadata-source"), "Mine"));
                        Assert.Equal("Items", Get<string>(Get<BlittableJsonReaderObject>(json, "@metadata"), "@collection"));

                        Assert.False(json.TryGet("_id", out object _));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport, Requires = RavenServiceRequirement.MongoDB)]
        public async Task MigrateMongodb_ShouldKeepObjectIdsAndGuidsAndPrefixOtherIdsWithCollectionName()
        {
            var objectId = ObjectId.GenerateNewId();
            var legacyGuid = Guid.NewGuid();

            using (var mongo = new MongoTestDatabase())
            {
                await mongo.Database.GetCollection<BsonDocument>("Users").InsertManyAsync(new[]
                {
                    new BsonDocument { { "_id", objectId }, { "Name", "objectId" } },
                    new BsonDocument { { "_id", new BsonBinaryData(TestGuid, GuidRepresentation.Standard) }, { "Name", "standardGuid" } },
                    new BsonDocument { { "_id", new BsonBinaryData(legacyGuid, GuidRepresentation.CSharpLegacy) }, { "Name", "legacyGuid" } },
                    new BsonDocument { { "_id", 1 }, { "Name", "int" } },
                    new BsonDocument { { "_id", 2L }, { "Name", "long" } },
                    new BsonDocument { { "_id", "abc" }, { "Name", "string" } },
                    new BsonDocument { { "_id", new BsonDocument { { "a", 1 }, { "b", 2 } } }, { "Name", "composite1" } },
                    new BsonDocument { { "_id", new BsonDocument { { "a", 1 }, { "b", 3 } } }, { "Name", "composite2" } }
                });

                // the same _id in another collection
                await mongo.Database.GetCollection<BsonDocument>("Orders").InsertOneAsync(new BsonDocument { { "_id", 1 }, { "Name", "order" } });

                using (var store = GetDocumentStore())
                {
                    await MigrateFromMongoDB(store, mongo);

                    var expected = new Dictionary<string, string>
                    {
                        [objectId.ToString()] = "objectId",
                        [TestGuid.ToString()] = "standardGuid",
                        [legacyGuid.ToString()] = "legacyGuid",
                        ["Users/1"] = "int",
                        ["Users/2"] = "long",
                        ["Users/abc"] = "string",
                        ["Users/{\"a\":1,\"b\":2}"] = "composite1",
                        ["Users/{\"a\":1,\"b\":3}"] = "composite2",
                        ["Orders/1"] = "order"
                    };

                    using (var commands = store.Commands())
                    {
                        foreach (var (id, name) in expected)
                        {
                            var document = await commands.GetAsync(id);
                            Assert.True(document != null, $"Document '{id}' wasn't migrated");
                            Assert.Equal(name, Get<string>(document.BlittableJson, "Name"));
                        }

                        // ObjectId and UUID identifiers are used as is, the original value of the other identifiers is kept
                        foreach (var id in new[] { objectId.ToString(), TestGuid.ToString(), legacyGuid.ToString() })
                        {
                            var document = await commands.GetAsync(id);
                            Assert.False(document.BlittableJson.TryGet("@mongodb-id", out object _), $"Document '{id}' shouldn't have the original id");
                        }

                        Assert.Equal(1, Get<long>((await commands.GetAsync("Users/1")).BlittableJson, "@mongodb-id"));
                        Assert.Equal(2, Get<long>((await commands.GetAsync("Users/2")).BlittableJson, "@mongodb-id"));
                        Assert.Equal("abc", Get<string>((await commands.GetAsync("Users/abc")).BlittableJson, "@mongodb-id"));

                        var compositeId = Get<BlittableJsonReaderObject>((await commands.GetAsync("Users/{\"a\":1,\"b\":2}")).BlittableJson, "@mongodb-id");
                        Assert.Equal(1, Get<long>(compositeId, "a"));
                        Assert.Equal(2, Get<long>(compositeId, "b"));
                    }

                    var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(expected.Count, stats.CountOfDocuments);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport, Requires = RavenServiceRequirement.MongoDB)]
        public async Task MigrateMongodb_WhenIdsCollideInRavenDB_ShouldMigrateAllDocuments()
        {
            using (var mongo = new MongoTestDatabase())
            {
                // distinct in MongoDB (typed and case-sensitive), the same in RavenDB (case-insensitive)
                await mongo.Database.GetCollection<BsonDocument>("Users").InsertManyAsync(new[]
                {
                    new BsonDocument { { "_id", 1 }, { "Name", "int" } },
                    new BsonDocument { { "_id", "1" }, { "Name", "string" } },
                    new BsonDocument { { "_id", "abc" }, { "Name", "lower" } },
                    new BsonDocument { { "_id", "ABC" }, { "Name", "upper" } },
                    new BsonDocument { { "_id", "1-2" }, { "Name", "suffix" } }
                });

                // a regular collection with the same name as a GridFS bucket
                await mongo.Database.GetCollection<BsonDocument>("photos").InsertOneAsync(new BsonDocument { { "_id", "p1" }, { "Name", "regular" } });
                var photosBucket = new GridFSBucket<string>(mongo.Database, new GridFSBucketOptions { BucketName = "photos" });
                await photosBucket.UploadFromBytesAsync("p1", "p.jpg", Encoding.UTF8.GetBytes("photo"));

                using (var store = GetDocumentStore())
                {
                    await MigrateFromMongoDB(store, mongo, migrateGridFS: true, collectionsToMigrate: new[] { ("Users", (string)null), ("photos", null) });

                    var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(7, stats.CountOfDocuments);

                    var originalIds = new Dictionary<string, object>
                    {
                        ["int"] = 1L,
                        ["string"] = "1",
                        ["lower"] = "abc",
                        ["upper"] = "ABC",
                        ["suffix"] = "1-2",
                        ["regular"] = "p1"
                    };

                    using (var commands = store.Commands())
                    {
                        var names = new List<string>();
                        foreach (var id in new[] { "Users/1", "Users/1-2", "Users/1-2-2", "Users/abc", "Users/abc-2", "photos/p1" })
                        {
                            var document = await commands.GetAsync(id);
                            Assert.True(document != null, $"Document '{id}' wasn't migrated");
                            var name = Get<string>(document.BlittableJson, "Name");
                            names.Add(name);

                            // the original id tells which MongoDB document got the suffixed id
                            var expectedOriginalId = originalIds[name];
                            if (expectedOriginalId is long number)
                                Assert.Equal(number, Get<long>(document.BlittableJson, "@mongodb-id"));
                            else
                                Assert.Equal(expectedOriginalId, Get<string>(document.BlittableJson, "@mongodb-id"));
                        }

                        Assert.Equal(new[] { "int", "lower", "regular", "string", "suffix", "upper" }, names.OrderBy(x => x));
                    }

                    await AssertGridFSFile(store, "photos/p1-2", "p.jpg", null, Encoding.UTF8.GetBytes("photo"),
                        file => Assert.Equal("p1", Get<string>(file, "@mongodb-id")));
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport, Requires = RavenServiceRequirement.MongoDB)]
        public async Task MigrateMongodb_ShouldMigrateOnlySelectedCollectionsWithNewNames()
        {
            using (var mongo = new MongoTestDatabase())
            {
                await mongo.Database.GetCollection<BsonDocument>("Books").InsertOneAsync(new BsonDocument { { "_id", 1 }, { "Title", "book" } });
                await mongo.Database.GetCollection<BsonDocument>("Orders").InsertOneAsync(new BsonDocument { { "_id", 1 }, { "Title", "order" } });
                await mongo.Database.GetCollection<BsonDocument>("Movies").InsertOneAsync(new BsonDocument { { "_id", 1 }, { "Title", "movie" } });

                using (var store = GetDocumentStore())
                {
                    await MigrateFromMongoDB(store, mongo, collectionsToMigrate: new[]
                    {
                        ("Books", "MyBooks"),
                        ("Orders", null)
                    });

                    var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                    Assert.Equal(2, collectionStats.CountOfDocuments);
                    Assert.Equal(1, collectionStats.Collections["MyBooks"]);
                    Assert.Equal(1, collectionStats.Collections["Orders"]);

                    using (var commands = store.Commands())
                    {
                        // the ID prefix uses the new collection name
                        Assert.NotNull(await commands.GetAsync("MyBooks/1"));
                        Assert.NotNull(await commands.GetAsync("Orders/1"));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport, Requires = RavenServiceRequirement.MongoDB)]
        public async Task MigrateMongodb_ShouldMigrateGridFSFilesFromAllBuckets()
        {
            var owner = ObjectId.GenerateNewId();
            var largeContent = new byte[5000];
            new Random(1).NextBytes(largeContent);

            using (var mongo = new MongoTestDatabase())
            {
                var bucket = new GridFSBucket(mongo.Database);
                var withContentTypeInMetadata = await bucket.UploadFromBytesAsync("a.txt", Encoding.UTF8.GetBytes("first file"),
                    new GridFSUploadOptions { Metadata = new BsonDocument { { "contentType", "text/plain" }, { "Owner", owner } } });
                var withTopLevelContentType = await bucket.UploadFromBytesAsync("b.csv", Encoding.UTF8.GetBytes("second file"),
                    new GridFSUploadOptions { Metadata = new BsonDocument { { "Owner", owner } } });
                var withoutMetadata = await bucket.UploadFromBytesAsync("c.bin", Encoding.UTF8.GetBytes("third file"));
                var withDateAndGuidInMetadata = await bucket.UploadFromBytesAsync("d.json", Encoding.UTF8.GetBytes("fourth file"),
                    new GridFSUploadOptions
                    {
                        Metadata = new BsonDocument
                        {
                            { "Content-Type", "application/json" },
                            { "Created", new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc) },
                            { "Ref", new BsonBinaryData(TestGuid, GuidRepresentation.Standard) }
                        }
                    });
                var multipleChunks = await bucket.UploadFromBytesAsync("e.dat", largeContent, new GridFSUploadOptions { ChunkSizeBytes = 1024 });
                var withoutFilename = await bucket.UploadFromBytesAsync(string.Empty, Encoding.UTF8.GetBytes("fifth file"));

                // the deprecated top-level 'contentType' field written by older drivers
                await mongo.Database.GetCollection<BsonDocument>("fs.files").UpdateOneAsync(
                    new BsonDocument("_id", withTopLevelContentType),
                    new BsonDocument("$set", new BsonDocument("contentType", "text/csv")));

                // a custom bucket with a non ObjectId file id
                var photosBucket = new GridFSBucket<string>(mongo.Database, new GridFSBucketOptions { BucketName = "photos" });
                await photosBucket.UploadFromBytesAsync("photo-1", "p.jpg", Encoding.UTF8.GetBytes("photo"),
                    new GridFSUploadOptions { Metadata = new BsonDocument("ContentType", "image/jpeg") });

                // a regular collection, not a GridFS bucket
                await mongo.Database.GetCollection<BsonDocument>("report.files").InsertOneAsync(new BsonDocument { { "_id", "r1" }, { "Note", "not GridFS" } });

                using (var store = GetDocumentStore())
                {
                    await MigrateFromMongoDB(store, mongo, migrateGridFS: true);

                    await AssertGridFSFile(store, withContentTypeInMetadata.ToString(), "a.txt", "text/plain", Encoding.UTF8.GetBytes("first file"),
                        file => Assert.Equal(owner.ToString(), Get<string>(file, "Owner")));
                    await AssertGridFSFile(store, withTopLevelContentType.ToString(), "b.csv", "text/csv", Encoding.UTF8.GetBytes("second file"),
                        file => Assert.Equal(owner.ToString(), Get<string>(file, "Owner")));
                    await AssertGridFSFile(store, withoutMetadata.ToString(), "c.bin", null, Encoding.UTF8.GetBytes("third file"));
                    await AssertGridFSFile(store, withDateAndGuidInMetadata.ToString(), "d.json", "application/json", Encoding.UTF8.GetBytes("fourth file"), file =>
                    {
                        Assert.Equal("2020-01-02T03:04:05Z", Get<string>(file, "Created"));
                        Assert.Equal(TestGuid.ToString(), Get<string>(file, "Ref"));
                    });
                    await AssertGridFSFile(store, multipleChunks.ToString(), "e.dat", null, largeContent);
                    // the document ID is used as the attachment name when the file has no name
                    await AssertGridFSFile(store, withoutFilename.ToString(), withoutFilename.ToString(), null, Encoding.UTF8.GetBytes("fifth file"));
                    // non ObjectId file ids are prefixed with the collection (bucket) name
                    await AssertGridFSFile(store, "photos/photo-1", "p.jpg", "image/jpeg", Encoding.UTF8.GetBytes("photo"));

                    var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                    Assert.Equal(6, collectionStats.Collections["Files"]);
                    Assert.Equal(1, collectionStats.Collections["photos"]);
                    Assert.Equal(1, collectionStats.Collections["report.files"]);
                    Assert.Equal(3, collectionStats.Collections.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport, Requires = RavenServiceRequirement.MongoDB)]
        public async Task MigrateMongodb_WhenNoCollectionsSelected_ShouldMigrateOnlyGridFS()
        {
            using (var mongo = new MongoTestDatabase())
            {
                await mongo.Database.GetCollection<BsonDocument>("Books").InsertOneAsync(new BsonDocument("Title", "book"));
                var fileId = await new GridFSBucket(mongo.Database).UploadFromBytesAsync("a.txt", Encoding.UTF8.GetBytes("file"));

                using (var store = GetDocumentStore())
                {
                    await MigrateFromMongoDB(store, mongo, migrateGridFS: true, collectionsToMigrate: Array.Empty<(string, string)>());

                    var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                    Assert.Equal(1, collectionStats.CountOfDocuments);
                    Assert.Equal(1, collectionStats.Collections["Files"]);

                    await AssertGridFSFile(store, fileId.ToString(), "a.txt", null, Encoding.UTF8.GetBytes("file"));
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport, Requires = RavenServiceRequirement.MongoDB)]
        public async Task MigrateMongodb_ThroughServer_ShouldListDatabasesAndCollectionsAndExportWithTransformScript()
        {
            using (var mongo = new MongoTestDatabase())
            {
                var expectedCollections = new List<string> { "Books", "report.files" };

                await mongo.Database.GetCollection<BsonDocument>("Books").InsertManyAsync(new[]
                {
                    new BsonDocument { { "_id", 1 }, { "Title", "first" } },
                    new BsonDocument { { "_id", 2 }, { "Title", "second" } }
                });
                await mongo.Database.GetCollection<BsonDocument>("report.files").InsertOneAsync(new BsonDocument("Note", "not GridFS"));
                // views must not be migrated, their documents come from the source collection
                await mongo.Database.RunCommandAsync<BsonDocument>(new BsonDocument { { "create", "BooksView" }, { "viewOn", "Books" }, { "pipeline", new BsonArray() } });

                if (await mongo.GetServerMajorVersion() >= 5)
                {
                    // time series collections are listed, their internal 'system.buckets.*' collections are not
                    await mongo.Database.RunCommandAsync<BsonDocument>(new BsonDocument { { "create", "Measurements" }, { "timeseries", new BsonDocument("timeField", "Timestamp") } });
                    await mongo.Database.GetCollection<BsonDocument>("Measurements").InsertOneAsync(new BsonDocument { { "Timestamp", DateTime.UtcNow }, { "Value", 1 } });
                    expectedCollections.Add("Measurements");
                }

                using (var store = GetDocumentStore())
                {
                    var databasesInfo = await RunMigratorThroughServer<DatabasesInfoResult>(store, NewInputConfiguration(mongo, "databases"));
                    Assert.Contains(mongo.Name, databasesInfo.Databases);
                    Assert.DoesNotContain("admin", databasesInfo.Databases);
                    Assert.DoesNotContain("local", databasesInfo.Databases);
                    Assert.DoesNotContain("config", databasesInfo.Databases);

                    var collectionsInfo = await RunMigratorThroughServer<CollectionsInfoResult>(store, NewInputConfiguration(mongo, "collections"));
                    Assert.Equal(expectedCollections.OrderBy(x => x), collectionsInfo.Collections.OrderBy(x => x));
                    Assert.False(collectionsInfo.HasGridFS);

                    await new GridFSBucket(mongo.Database, new GridFSBucketOptions { BucketName = "photos" }).UploadFromBytesAsync("p.jpg", Encoding.UTF8.GetBytes("photo"));

                    collectionsInfo = await RunMigratorThroughServer<CollectionsInfoResult>(store, NewInputConfiguration(mongo, "collections"));
                    Assert.Equal(expectedCollections.OrderBy(x => x), collectionsInfo.Collections.OrderBy(x => x));
                    Assert.True(collectionsInfo.HasGridFS);

                    var operationIdResult = await RunMigratorThroughServer<OperationIdResult>(store, NewInputConfiguration(mongo, "export"), transformScript: "this.Migrated = true;");
                    var operation = new Operation(store.GetRequestExecutor(), () => store.Changes(store.Database, operationIdResult.OperationNodeTag), store.Conventions,
                        operationIdResult.OperationId, operationIdResult.OperationNodeTag);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
                    Assert.Equal(expectedCollections.OrderBy(x => x), collectionStats.Collections.Keys.OrderBy(x => x));
                    Assert.Equal(2, collectionStats.Collections["Books"]);

                    using (var commands = store.Commands())
                    {
                        var book = await commands.GetAsync("Books/1");
                        Assert.Equal("first", Get<string>(book.BlittableJson, "Title"));
                        Assert.True(Get<bool>(book.BlittableJson, "Migrated"));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task MigrateMongodb_ThroughServer_WhenMongoDBIsUnreachable_ShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                var inputConfiguration = new DynamicJsonValue
                {
                    ["Command"] = "databases",
                    ["ConnectionString"] = "mongodb://localhost:1/?serverSelectionTimeoutMS=1000&connectTimeoutMS=1000",
                    ["ConsoleExport"] = true
                };

                var e = await Assert.ThrowsAnyAsync<Exception>(() => RunMigratorThroughServer<DatabasesInfoResult>(store, inputConfiguration));
                Assert.Contains("Failed to run mongodb command", e.Message);
                Assert.Contains("localhost:1", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task MigrateMongodb_ThroughServer_ShouldValidateConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                // valid path
                await RunMigratorThroughServer<object>(store, new DynamicJsonValue { ["Command"] = "validateMigratorPath" });

                var e = await Assert.ThrowsAnyAsync<Exception>(() =>
                    RunMigratorThroughServer<object>(store, new DynamicJsonValue { ["Command"] = "validateMigratorPath" }, migratorFullPath: Path.Combine(NewDataPath(), "missing")));
                Assert.Contains("doesn't exist", e.Message);

                e = await Assert.ThrowsAnyAsync<Exception>(() =>
                    RunMigratorThroughServer<object>(store, new DynamicJsonValue { ["Command"] = "unknown", ["ConsoleExport"] = true }));
                Assert.Contains("Command 'unknown' doesn't exist", e.Message);

                e = await Assert.ThrowsAnyAsync<Exception>(() =>
                    RunMigratorThroughServer<object>(store, new DynamicJsonValue { ["Command"] = "collections", ["ConnectionString"] = "mongodb://localhost:1/", ["ConsoleExport"] = true }));
                Assert.Contains("DatabaseName cannot be null or empty", e.Message);

                e = await Assert.ThrowsAnyAsync<Exception>(() =>
                    RunMigratorThroughServer<object>(store, new DynamicJsonValue { ["Command"] = "databases", ["ConsoleExport"] = true }));
                Assert.Contains("ConnectionString cannot be null or empty", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void Migrator_WhenExportingToConsoleAndFile_ShouldFail()
        {
            var inputConfiguration = Newtonsoft.Json.JsonConvert.SerializeObject(new
            {
                Command = "export",
                ConnectionString = "mongodb://localhost:1/",
                DatabaseName = "db",
                ConsoleExport = true,
                ExportFilePath = NewDataPath()
            });

            // the connection string is unreachable as well, so the exit code alone doesn't prove the validation
            var originalError = Console.Error;
            var error = new StringWriter();
            Console.SetError(error);
            try
            {
                Assert.Equal(-1, Raven.Migrator.Program.Main(new[] { "mongodb", "-j", inputConfiguration }));
            }
            finally
            {
                Console.SetError(originalError);
            }

            Assert.Contains("Cannot export to the console and to a file", error.ToString());
        }

        private static async Task AssertGridFSFile(IDocumentStore store, string id, string expectedName, string expectedContentType, byte[] expectedContent,
            Action<BlittableJsonReaderObject> assertDocument = null)
        {
            using (var commands = store.Commands())
            {
                var file = await commands.GetAsync(id);
                Assert.True(file != null, $"GridFS file '{id}' wasn't migrated");

                var metadata = Get<BlittableJsonReaderObject>(file.BlittableJson, "@metadata");
                var attachments = Get<BlittableJsonReaderArray>(metadata, "@attachments");
                var attachmentInfo = (BlittableJsonReaderObject)Assert.Single(attachments);
                var attachmentName = Get<string>(attachmentInfo, "Name");
                Assert.Equal(expectedName, attachmentName);

                attachmentInfo.TryGet("ContentType", out string contentType);
                if (expectedContentType == null)
                    Assert.True(string.IsNullOrEmpty(contentType), $"Unexpected content type: {contentType}");
                else
                    Assert.Equal(expectedContentType, contentType);

                using (var attachment = await store.Operations.SendAsync(new GetAttachmentOperation(id, attachmentName, AttachmentType.Document, changeVector: null)))
                using (var memoryStream = new MemoryStream())
                {
                    await attachment.Stream.CopyToAsync(memoryStream);
                    Assert.Equal(expectedContent, memoryStream.ToArray());
                }

                assertDocument?.Invoke(file.BlittableJson);
            }
        }

        private async Task MigrateFromMongoDB(IDocumentStore store, MongoTestDatabase mongo, bool migrateGridFS = false, (string Name, string NewName)[] collectionsToMigrate = null)
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
                    ["DatabaseName"] = mongo.Name,
                    ["Command"] = "export",
                    ["ExportFilePath"] = path,
                    ["ConnectionString"] = mongo.ConnectionString,
                    ["MigrateGridFS"] = migrateGridFS,
                    ["CollectionsToMigrate"] = ToDynamicJsonArray(collectionsToMigrate)
                };
                var inputConfigurationStr = context.ReadObject(inputConfiguration, "InputConfiguration").ToString();

                Assert.Equal(0, Raven.Migrator.Program.Main(new[] { "mongodb", "-j", inputConfigurationStr }));
            }

            var databaseDumpFilePath = Assert.Single(Directory.GetFiles(path));
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), databaseDumpFilePath);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
        }

        private static DynamicJsonValue NewInputConfiguration(MongoTestDatabase mongo, string command)
        {
            return new DynamicJsonValue
            {
                ["Command"] = command,
                ["ConnectionString"] = mongo.ConnectionString,
                ["DatabaseName"] = mongo.Name,
                ["ConsoleExport"] = true
            };
        }

        private static DynamicJsonArray ToDynamicJsonArray((string Name, string NewName)[] collections)
        {
            if (collections == null)
                return null;

            return new DynamicJsonArray(collections.Select(x => new DynamicJsonValue
            {
                ["Name"] = x.Name,
                ["NewName"] = x.NewName
            }));
        }

        /// <summary>
        /// The endpoint used by the Studio: the server runs Raven.Migrator, passes the configuration through the standard input
        /// and imports the console export
        /// </summary>
        private static async Task<T> RunMigratorThroughServer<T>(IDocumentStore store, DynamicJsonValue inputConfiguration, string transformScript = null, string migratorFullPath = null)
        {
            var requestExecutor = store.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var configuration = context.ReadObject(new DynamicJsonValue
                {
                    ["DatabaseTypeName"] = "mongodb",
                    // Raven.Migrator is copied to the tests output directory
                    ["MigratorFullPath"] = migratorFullPath ?? AppContext.BaseDirectory,
                    ["InputConfiguration"] = inputConfiguration,
                    ["TransformScript"] = transformScript
                }, "migration-configuration");

                var command = new MigrateCommand<T>(configuration);
                await requestExecutor.ExecuteAsync(command, context);
                return command.Result;
            }
        }

        private static T Get<T>(BlittableJsonReaderObject json, string propertyName)
        {
            Assert.True(json.TryGet(propertyName, out T value), $"Property '{propertyName}' is missing or has an unexpected type");
            return value;
        }

        private static void AssertEquivalent<T>(IEnumerable<T> expected, IEnumerable<T> actual, IEqualityComparer<T> comparer)
        {
            Assert.Equal(expected.Count(), actual.Count());

            void Action(T b1) => Assert.Contains(b1, expected, comparer);

            var actions = expected.Select(b => (Action<T>)Action).ToArray();

            Assert.Collection(actual, actions);
        }

        private class DatabasesInfoResult
        {
            public List<string> Databases { get; set; }
        }

        private class CollectionsInfoResult
        {
            public List<string> Collections { get; set; }

            public bool HasGridFS { get; set; }
        }

        private class MigrateCommand<T> : RavenCommand<T>
        {
            private readonly BlittableJsonReaderObject _configuration;

            public MigrateCommand(BlittableJsonReaderObject configuration)
            {
                _configuration = configuration;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/smuggler/migrate";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _configuration).ConfigureAwait(false), DocumentConventions.Default)
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<T>(response);
            }
        }

        private sealed class MongoTestDatabase : IDisposable
        {
            public MongoTestDatabase()
            {
                ConnectionString = MongoDBConnectionString.Instance.ConnectionString.Value;
                Client = new MongoClient(ConnectionString);
                Name = $"Test{Guid.NewGuid():N}";
                Database = Client.GetDatabase(Name);
            }

            public string ConnectionString { get; }

            public MongoClient Client { get; }

            public string Name { get; }

            public IMongoDatabase Database { get; }

            public async Task<int> GetServerMajorVersion()
            {
                var buildInfo = await Database.RunCommandAsync<BsonDocument>(new BsonDocument("buildInfo", 1));
                return buildInfo["versionArray"][0].ToInt32();
            }

            public void Dispose()
            {
                Client.DropDatabase(Name);
                Client.Dispose();
            }
        }
    }
}
