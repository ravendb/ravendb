using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Raven.Migrator.CosmosDB
{
    public class CosmosDBMigrator : INoSqlMigrator
    {
        private readonly CosmosDBConfiguration _configuration;

        private static readonly List<string> PropertiesToRemove = new List<string>
        {
            "Id",
            "_rid",
            "_etag",
            "_ts",
            "_attachments",
            "_self"
        };

        private const string CosmosDocumentId = "id";

        public CosmosDBMigrator(CosmosDBConfiguration configuration)
        {
            _configuration = configuration;
        }

        public Task GetDatabases()
        {
            var databases = new List<string>();

            var client = CreateNewCosmosClient();
            var azureDatabases = client.CreateDatabaseQuery().AsEnumerable().ToList();
            foreach (var azureDatabase in azureDatabases)
            {
                databases.Add(azureDatabase.Id);
            }

            MigrationHelpers.OutputClass(_configuration,
                new DatabasesInfo
                {
                    Databases = databases
                });

            return Task.CompletedTask;
        }

        public async Task GetCollectionsInfo()
        {
            AssertDatabaseName();

            var client = CreateNewCosmosClient();
            var databaseUri = UriFactory.CreateDatabaseUri(_configuration.DatabaseName);
            var database = (await client.ReadDatabaseAsync(databaseUri)).Resource;
            var collections = GetCollections(client, database).ToList();

            MigrationHelpers.OutputClass(_configuration,
                new CollectionsInfo
                {
                    Collections = collections
                });
        }

        private static IEnumerable<string> GetCollections(DocumentClient client, Database database)
        {
            var azureCollections = client.CreateDocumentCollectionQuery(database.SelfLink);
            foreach (var azureCollection in azureCollections)
            {
                yield return azureCollection.Id;
            }
        }

        public async Task MigrateDatabse()
        {
            AssertDatabaseName();

            var client = CreateNewCosmosClient();
            var databaseUri = UriFactory.CreateDatabaseUri(_configuration.DatabaseName);
            var database = (await client.ReadDatabaseAsync(databaseUri)).Resource;

            if (_configuration.CollectionsToMigrate == null ||
                _configuration.CollectionsToMigrate.Count == 0)
            {
                _configuration.CollectionsToMigrate = GetCollectionsToMigrate(client, database);
            }

            await MigrationHelpers.MigrateNoSqlDatabase(
                _configuration,
                async (mongoCollectionName, ravenCollectionName, isFirstDocument, streamWriter) =>
                    await MigrateSingleCollection(client, database, mongoCollectionName, ravenCollectionName, isFirstDocument, streamWriter));
        }

        private static async Task MigrateSingleCollection(
            DocumentClient client,
            Database database,
            string cosmosCollectionName,
            string ravenCollectionName,
            Reference<bool> isFirstDocument,
            StreamWriter streamWriter)
        {
            var collection = UriFactory.CreateDocumentCollectionUri(database.Id, cosmosCollectionName);
            var options = new FeedOptions
            {
                MaxItemCount = 128
            };

            var query = client.CreateDocumentQuery<ExpandoObject>(collection, options).AsDocumentQuery();
            var attachmentNumber = 0;

            while (query.HasMoreResults)
            {
                var result = await query.ExecuteNextAsync<ExpandoObject>();
                foreach (var document in result)
                {
                    var dictionary = (IDictionary<string, object>)document;
                    var documentId = dictionary[CosmosDocumentId].ToString();

                    const string selfLinkKey = "_self";
                    var selfLink = dictionary[selfLinkKey].ToString();

                    var attachments = new List<Dictionary<string, object>>();
                    foreach (var attachment in client.CreateAttachmentQuery(selfLink))
                    {
                        var attachmentUri = UriFactory.CreateAttachmentUri(database.Id, cosmosCollectionName, documentId, attachment.Id);
                        var attachmentResponse = await client.ReadAttachmentAsync(attachmentUri);
                        var resourceMediaLink = attachmentResponse.Resource.MediaLink;
                        var mediaLinkResponse = await client.ReadMediaAsync(resourceMediaLink);

                        attachmentNumber++;

                        var attachmentInfo = await MigrationHelpers.WriteAttachment(
                            mediaLinkResponse.Media, mediaLinkResponse.Media.Length,
                            attachmentResponse.Resource.Id, ravenCollectionName,
                            mediaLinkResponse.ContentType,attachmentNumber, isFirstDocument, streamWriter);

                        attachments.Add(attachmentInfo);
                    }

                    await MigrationHelpers.WriteDocument(
                        document,
                        documentId,
                        ravenCollectionName,
                        PropertiesToRemove,
                        isFirstDocument,
                        streamWriter,
                        attachments);
                }
            }
        }

        private static List<Collection> GetCollectionsToMigrate(DocumentClient client, Database database)
        {
            var collectionsToMigrate = new List<Collection>();

            foreach (var azureCollection in GetCollections(client, database))
            {
                collectionsToMigrate.Add(new Collection
                {
                    Name = azureCollection,
                    NewName = azureCollection
                });
            }

            return collectionsToMigrate;
        }

        private void AssertDatabaseName()
        {
            if (string.IsNullOrWhiteSpace(_configuration.DatabaseName))
                throw new ArgumentException("DatabaseName cannot be null or empty");
        }

        private DocumentClient CreateNewCosmosClient()
        {
            return new DocumentClient(new Uri(_configuration.AzureEndpointUrl), _configuration.PrimaryKey);
        }
    }
}
