using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using Newtonsoft.Json;

namespace Raven.Migrator.MongoDB
{
    public class MongoDBMigrator : INoSqlMigrator
    {
        private readonly MongoDBConfiguration _configuration;

        private const string MongoDocumentId = "_id";

        private const string DefaultGridFSBucketName = "fs";
        private const string DefaultGridFSCollectionName = "Files";
        private const string GridFSFilesSuffix = ".files";
        private const string GridFSChunksSuffix = ".chunks";
        private const string SystemCollectionPrefix = "system.";

        // larger GridFS files are buffered in a temporary file instead of memory
        private const long MaxInMemoryGridFSFileSize = 64 * 1024 * 1024;

        private readonly List<string> _propertiesToRemove = new List<string>
        {
            MongoDocumentId
        };

        private readonly HashSet<ulong> _usedDocumentIdHashes = new HashSet<ulong>();

        public MongoDBMigrator(MongoDBConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task GetDatabases()
        {
            AssertConnectionString();

            var databases = new List<string>();

            var client = CreateNewMongoClient();
            AssertOnline(client, client.GetDatabase("database"));

            using (var cursor = await client.ListDatabasesAsync())
            {
                while (await cursor.MoveNextAsync())
                {
                    foreach (var database in cursor.Current)
                    {
                        var name = database["name"].ToString();
                        if (name.Equals("admin") || name.Equals("local") || name.Equals("config"))
                            continue;

                        databases.Add(name);
                    }
                }
            }

            MigrationHelpers.OutputClass(_configuration,
                new DatabasesInfo
                {
                    Databases = databases
                });
        }

        public async Task GetCollectionsInfo()
        {
            AssertDatabaseName();

            var client = CreateNewMongoClient();
            var database = client.GetDatabase(_configuration.DatabaseName);
            AssertOnline(client, database);

            var databaseCollections = await GetDatabaseCollections(database);

            MigrationHelpers.OutputClass(_configuration,
                new ExtendedCollectionsInfo
                {
                    Collections = databaseCollections.Collections,
                    HasGridFS = await HasGridFS(database, databaseCollections.GridFSBuckets)
                });
        }

        private static async Task<DatabaseCollections> GetDatabaseCollections(IMongoDatabase database)
        {
            var names = new List<string>();

            using (var cursor = await database.ListCollectionsAsync())
            {
                while (await cursor.MoveNextAsync())
                {
                    foreach (var collectionDocument in cursor.Current)
                    {
                        // views are skipped, their documents are migrated from the source collections
                        var type = collectionDocument.GetValue("type", "collection").AsString;
                        if (type != "collection" && type != "timeseries")
                            continue;

                        var collectionName = collectionDocument["name"].AsString;
                        if (collectionName.StartsWith(SystemCollectionPrefix, StringComparison.Ordinal))
                            continue;

                        names.Add(collectionName);
                    }
                }
            }

            var allNames = new HashSet<string>(names, StringComparer.Ordinal);
            var gridFSBuckets = new List<string>();
            var gridFSCollections = new HashSet<string>(StringComparer.Ordinal);

            foreach (var name in names)
            {
                if (name.EndsWith(GridFSFilesSuffix, StringComparison.Ordinal) == false)
                    continue;

                var bucketName = name.Substring(0, name.Length - GridFSFilesSuffix.Length);
                var chunksCollectionName = bucketName + GridFSChunksSuffix;
                if (allNames.Contains(chunksCollectionName) == false)
                    continue; // a regular collection that happens to end with '.files'

                gridFSBuckets.Add(bucketName);
                gridFSCollections.Add(name);
                gridFSCollections.Add(chunksCollectionName);
            }

            return new DatabaseCollections
            {
                Collections = names.Where(x => gridFSCollections.Contains(x) == false).ToList(),
                GridFSBuckets = gridFSBuckets
            };
        }

        private static async Task<bool> HasGridFS(IMongoDatabase database, List<string> gridFSBuckets)
        {
            foreach (var bucketName in gridFSBuckets)
            {
                var bucket = CreateGridFSBucket(database, bucketName);
                using (var cursor = await bucket.FindAsync(Builders<GridFSFileInfo<BsonValue>>.Filter.Empty, new GridFSFindOptions<BsonValue> { Limit = 1 }))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        if (cursor.Current.Any())
                            return true;
                    }
                }
            }

            return false;
        }

        public async Task MigrateDatabase()
        {
            AssertDatabaseName();

            var client = CreateNewMongoClient();
            var database = client.GetDatabase(_configuration.DatabaseName);
            AssertOnline(client, database);

            var databaseCollections = await GetDatabaseCollections(database);

            if (_configuration.CollectionsToMigrate == null ||
                _configuration.CollectionsToMigrate.Count == 0 && _configuration.MigrateGridFS == false)
            {
                _configuration.CollectionsToMigrate = databaseCollections.Collections
                    .Select(x => new Collection
                    {
                        Name = x,
                        NewName = x
                    }).ToList();
            }

            await MigrationHelpers.MigrateNoSqlDatabase(
                _configuration,
                async (mongoCollectionName, ravenCollectionName, jsonTextWriter, streamWriter) =>
                    await MigrateSingleCollection(database, mongoCollectionName, ravenCollectionName, jsonTextWriter),
                async (jsonTextWriter, streamWriter) =>
                {
                    if (_configuration.MigrateGridFS == false)
                        return;

                    foreach (var bucketName in databaseCollections.GridFSBuckets)
                    {
                        await MigrateGridFS(database, bucketName, jsonTextWriter, streamWriter);
                    }
                });
        }

        private async Task MigrateSingleCollection(
            IMongoDatabase database,
            string mongoCollectionName,
            string ravenCollectionName,
            JsonTextWriter jsonTextWriter)
        {
            var collection = database.GetCollection<BsonDocument>(mongoCollectionName);

            using (var documentsCursor = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToCursorAsync())
            {
                while (await documentsCursor.MoveNextAsync())
                {
                    foreach (var bsonDocument in documentsCursor.Current)
                    {
                        var documentId = GetDocumentId(bsonDocument.GetValue(MongoDocumentId, BsonNull.Value), ravenCollectionName);
                        var document = MongoBsonConverter.ToExpandoObject(bsonDocument);

                        MigrationHelpers.WriteDocument(
                            document,
                            documentId,
                            ravenCollectionName,
                            _propertiesToRemove,
                            jsonTextWriter);
                    }
                }
            }
        }

        /// <summary>
        /// ObjectId and UUID identifiers are globally unique and are used as is (so references stored in other documents still match),
        /// other identifiers (numbers, strings, embedded documents) are prefixed with the collection name, e.g. 'Users/1',
        /// to avoid collisions between collections.
        /// </summary>
        private string GetDocumentId(BsonValue mongoId, string ravenCollectionName)
        {
            switch (mongoId.BsonType)
            {
                case BsonType.ObjectId:
                    return mongoId.AsObjectId.ToString();
                case BsonType.Binary when MongoBsonConverter.TryGetGuid(mongoId.AsBsonBinaryData, out var guid):
                    return guid.ToString();
                case BsonType.Null:
                case BsonType.Undefined:
                    return $"{ravenCollectionName}/{Guid.NewGuid()}";
                default:
                    return GetUniqueDocumentId($"{ravenCollectionName}/{MongoBsonConverter.ToIdString(mongoId)}");
            }
        }

        /// <summary>
        /// RavenDB document IDs are case-insensitive while MongoDB identifiers are case-sensitive and typed (1 and '1' can coexist),
        /// so an ID that was already used gets a numeric suffix, e.g. 'Users/abc' and 'Users/ABC-2'.
        /// Only hashes of the used IDs are kept, a hash collision just adds a suffix to an ID that didn't need one.
        /// </summary>
        private string GetUniqueDocumentId(string documentId)
        {
            if (_usedDocumentIdHashes.Add(GetDocumentIdHash(documentId)))
                return documentId;

            for (var suffix = 2; ; suffix++)
            {
                var uniqueDocumentId = $"{documentId}-{suffix}";
                if (_usedDocumentIdHashes.Add(GetDocumentIdHash(uniqueDocumentId)))
                    return uniqueDocumentId;
            }
        }

        private static ulong GetDocumentIdHash(string documentId)
        {
            // 64-bit FNV-1a of the lower-cased ID
            var hash = 14695981039346656037UL;
            foreach (var c in documentId)
            {
                hash ^= char.ToLowerInvariant(c);
                hash *= 1099511628211UL;
            }

            return hash;
        }

        private async Task MigrateGridFS(
            IMongoDatabase database,
            string bucketName,
            JsonTextWriter jsonTextWriter,
            StreamWriter streamWriter)
        {
            var bucket = CreateGridFSBucket(database, bucketName);
            var collectionName = bucketName == DefaultGridFSBucketName ? DefaultGridFSCollectionName : bucketName;
            var filter = Builders<GridFSFileInfo<BsonValue>>.Filter.Empty;
            var attachmentNumber = 0;

            using (var cursor = await bucket.FindAsync(filter))
            {
                while (await cursor.MoveNextAsync())
                {
                    foreach (var fileInfo in cursor.Current)
                    {
                        var document = MongoBsonConverter.ToExpandoObject(fileInfo.Metadata ?? new BsonDocument());
                        var dictionary = (IDictionary<string, object>)document;
                        var documentId = GetDocumentId(fileInfo.Id, collectionName);
                        var totalSize = fileInfo.Length;
                        var contentType = GetContentType(fileInfo, dictionary);
                        var attachmentName = string.IsNullOrWhiteSpace(fileInfo.Filename) ? documentId : fileInfo.Filename;

                        //can't OpenDownloadStreamAsync because it's not a seekable stream
                        await using (var stream = CreateGridFSFileBuffer(totalSize))
                        {
                            await bucket.DownloadToStreamAsync(fileInfo.Id, stream);
                            stream.Position = 0;

                            attachmentNumber++;
                            var attachmentInfo = await MigrationHelpers.WriteAttachment(
                                stream, totalSize, attachmentName, collectionName, contentType,
                                attachmentNumber, jsonTextWriter, streamWriter);

                            MigrationHelpers.WriteDocument(
                                document,
                                documentId,
                                collectionName,
                                null,
                                jsonTextWriter,
                                new List<Dictionary<string, object>>
                                {
                                    attachmentInfo
                                });
                        }
                    }
                }
            }
        }

        private static GridFSBucket<BsonValue> CreateGridFSBucket(IMongoDatabase database, string bucketName)
        {
            return new GridFSBucket<BsonValue>(database, new GridFSBucketOptions
            {
                BucketName = bucketName
            });
        }

        private static Stream CreateGridFSFileBuffer(long size)
        {
            if (size <= MaxInMemoryGridFSFileSize)
                return new MemoryStream();

            return new FileStream(Path.GetTempFileName(), FileMode.Create, FileAccess.ReadWrite, FileShare.None, 4096,
                FileOptions.DeleteOnClose | FileOptions.Asynchronous);
        }

        private static string GetContentType(GridFSFileInfo<BsonValue> fileInfo, IDictionary<string, object> dictionary)
        {
            // the deprecated top-level 'contentType' field of the files collection document
            // (GridFSFileInfo.ContentType was removed in MongoDB.Driver 3.x)
            string contentType = null;
            if (fileInfo.BackingDocument.TryGetValue("contentType", out var contentTypeValue) && contentTypeValue.IsString)
                contentType = contentTypeValue.AsString;

            if (dictionary.TryGetValue("contentType", out object value) && value is string metadataContentType)
            {
                contentType = metadataContentType;
            }
            else if (dictionary.TryGetValue("Content-Type", out value) && value is string metadataHeaderContentType)
            {
                contentType = metadataHeaderContentType;
            }
            else if (dictionary.TryGetValue("ContentType", out value) && value is string metadataPascalContentType)
            {
                contentType = metadataPascalContentType;
            }

            return contentType;
        }

        private void AssertConnectionString()
        {
            if (string.IsNullOrWhiteSpace(_configuration.ConnectionString))
                throw new ArgumentException("ConnectionString cannot be null or empty");
        }

        private void AssertDatabaseName()
        {
            if (string.IsNullOrWhiteSpace(_configuration.DatabaseName))
                throw new ArgumentException("DatabaseName cannot be null or empty");
        }

        private MongoClient CreateNewMongoClient()
        {
            return new MongoClient(_configuration.ConnectionString);
        }

        public void AssertOnline(MongoClient client, IMongoDatabase database)
        {
            var online = database.RunCommandAsync((Command<BsonDocument>)"{ping:1}").Wait(TimeSpan.FromSeconds(5));
            if (online == false)
                throw new InvalidOperationException($"Couldn't connect to: {_configuration.ConnectionString}");
        }

        private sealed class DatabaseCollections
        {
            public List<string> Collections { get; set; }

            public List<string> GridFSBuckets { get; set; }
        }
    }
}
