// -----------------------------------------------------------------------
//  <copyright file="SyncCommandsController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Web.Models.Indexes;

namespace Raven.Tests.Web.Controllers.Commands
{
    public class AsyncCommandsController : RavenApiController
    {
        [Route("api/async/commands/batch")]
        public async Task<HttpResponseMessage> Batch()
        {
            await DocumentStore.AsyncDatabaseCommands.BatchAsync(new List<ICommandData>
                                                 {
                                                     new PutCommandData
                                                     {
                                                         Document = new RavenJObject(), 
                                                         Key = Guid.NewGuid().ToString(), 
                                                         Metadata = new RavenJObject()
                                                     }
                                                 }.ToArray());

            return new HttpResponseMessage();
        }

        [Route("api/async/commands/delete")]
        public async Task<HttpResponseMessage> Delete()
        {
            await DocumentStore.AsyncDatabaseCommands.DeleteAsync("keys/1", null);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/deleteAttachment")]
        public async Task<HttpResponseMessage> DeleteAttachment()
        {
#pragma warning disable 618
            await DocumentStore.AsyncDatabaseCommands.DeleteAttachmentAsync("keys/1", null);
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/deleteByIndex")]
        public async Task<HttpResponseMessage> DeleteByIndex()
        {
            var operation = await DocumentStore.AsyncDatabaseCommands.DeleteByIndexAsync(new RavenDocumentsByEntityName().IndexName, new IndexQuery { Query = "Tag:Orders" }, new BulkOperationOptions { AllowStale = true });
            await operation.WaitForCompletionAsync();
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/deleteIndex")]
        public async Task<HttpResponseMessage> DeleteIndex()
        {
            await DocumentStore.AsyncDatabaseCommands.DeleteIndexAsync("index1");
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/deleteTransformer")]
        public async Task<HttpResponseMessage> DeleteTransformer()
        {
            await DocumentStore.AsyncDatabaseCommands.DeleteTransformerAsync("transformer1");
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/get")]
        public async Task<HttpResponseMessage> Get()
        {
            await DocumentStore.AsyncDatabaseCommands.GetAsync("keys/1");
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/get2")]
        public async Task<HttpResponseMessage> Get2()
        {
            await DocumentStore.AsyncDatabaseCommands.GetAsync(new[] { "keys/1", "keys/2" }, null);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getAttachment")]
        public async Task<HttpResponseMessage> GetAttachment()
        {
#pragma warning disable 618
            await DocumentStore.AsyncDatabaseCommands.GetAttachmentAsync("attachment1");
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getAttachmentHeadersStartingWith")]
        public async Task<HttpResponseMessage> GetAttachmentHeadersStartingWith()
        {
#pragma warning disable 618
            await DocumentStore.AsyncDatabaseCommands.GetAttachmentHeadersStartingWithAsync("attachments", 0, 128);
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getAttachments")]
        public async Task<HttpResponseMessage> GetAttachments()
        {
#pragma warning disable 618
            await DocumentStore.AsyncDatabaseCommands.GetAttachmentsAsync(0, Etag.Empty, 128);
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getBulkInsertOperation")]
        public async Task<HttpResponseMessage> GetBulkInsertOperation()
        {
            using (var operation = DocumentStore.AsyncDatabaseCommands.GetBulkInsertOperation(new BulkInsertOptions(), DocumentStore.Changes()))
            {
                await operation.WriteAsync(Guid.NewGuid().ToString(), new RavenJObject(), new RavenJObject());
            }

            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getDocuments1")]
        public async Task<HttpResponseMessage> GetDocuments()
        {
            await DocumentStore.AsyncDatabaseCommands.GetDocumentsAsync(Etag.Empty, 10);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getDocuments2")]
        public async Task<HttpResponseMessage> GetDocuments2()
        {
            await DocumentStore.AsyncDatabaseCommands.GetDocumentsAsync(0, 10);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getFacets")]
        public async Task<HttpResponseMessage> GetFacets()
        {
            await DocumentStore.AsyncDatabaseCommands.GetFacetsAsync(new RavenDocumentsByEntityName().IndexName, new IndexQuery(), new List<Facet> { new Facet { Name = "Facet1", Mode = FacetMode.Default } });
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getIndex")]
        public async Task<HttpResponseMessage> GetIndex()
        {
            await DocumentStore.AsyncDatabaseCommands.GetIndexAsync("index1");
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getIndexMergeSuggestions")]
        public async Task<HttpResponseMessage> GetIndexMergeSuggestions()
        {
            await DocumentStore.AsyncDatabaseCommands.GetIndexMergeSuggestionsAsync();
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getIndexNames")]
        public async Task<HttpResponseMessage> GetIndexNames()
        {
            await DocumentStore.AsyncDatabaseCommands.GetIndexNamesAsync(0, 10);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getIndexes")]
        public async Task<HttpResponseMessage> GetIndexes()
        {
            await DocumentStore.AsyncDatabaseCommands.GetIndexesAsync(0, 10);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getLicenseStatus")]
        public async Task<HttpResponseMessage> GetLicenseStatus()
        {
            await DocumentStore.AsyncDatabaseCommands.GetLicenseStatusAsync();
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getLogs")]
        public async Task<HttpResponseMessage> GetLogs()
        {
            await DocumentStore.AsyncDatabaseCommands.GetLogsAsync(false);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getMultiFacets")]
        public async Task<HttpResponseMessage> GetMultiFacets()
        {
            await DocumentStore.AsyncDatabaseCommands.GetMultiFacetsAsync(new[]
                                                          {
                                                              new FacetQuery
                                                              {
                                                                  IndexName = new RavenDocumentsByEntityName().IndexName, 
                                                                  Query = new IndexQuery(),
                                                                  Facets = new List<Facet>
                                                                           {
                                                                               new Facet
                                                                               {
                                                                                   Name = "Facet1", 
                                                                                   Mode = FacetMode.Default
                                                                               }
                                                                           }
                                                              }
                                                          });
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getStatistics")]
        public async Task<HttpResponseMessage> GetStatistics()
        {
            await DocumentStore.AsyncDatabaseCommands.GetStatisticsAsync();
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getTerms")]
        public async Task<HttpResponseMessage> GetTerms()
        {
            await DocumentStore.AsyncDatabaseCommands.GetTermsAsync(new RavenDocumentsByEntityName().IndexName, "Tag", string.Empty, 128);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getTransformer")]
        public async Task<HttpResponseMessage> GetTransformer()
        {
            await DocumentStore.AsyncDatabaseCommands.GetTransformerAsync("transformer1");
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/getTransformers")]
        public async Task<HttpResponseMessage> GetTransformers()
        {
            await DocumentStore.AsyncDatabaseCommands.GetTransformersAsync(0, 128);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/head")]
        public async Task<HttpResponseMessage> Head()
        {
            await DocumentStore.AsyncDatabaseCommands.HeadAsync("keys/1");
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/headAttachment")]
        public async Task<HttpResponseMessage> HeadAttachment()
        {
#pragma warning disable 618
            await DocumentStore.AsyncDatabaseCommands.HeadAttachmentAsync("keys/1");
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/indexHasChanged")]
        public async Task<HttpResponseMessage> IndexHasChanged()
        {
            await DocumentStore.AsyncDatabaseCommands.IndexHasChangedAsync(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/moreLikeThis")]
        public async Task<HttpResponseMessage> MoreLikeThis()
        {
            var key = Guid.NewGuid().ToString();
            DocumentStore.DatabaseCommands.GetStatistics();
            await DocumentStore.AsyncDatabaseCommands.PutAsync(key, null, new RavenJObject(), new RavenJObject());

            SpinWait.SpinUntil(() => DocumentStore.DatabaseCommands.GetStatistics().StaleIndexes.Length == 0);

            await DocumentStore.AsyncDatabaseCommands.MoreLikeThisAsync(new MoreLikeThisQuery { IndexName = new RavenDocumentsByEntityName().IndexName, DocumentId = key });
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/multiGet")]
        public async Task<HttpResponseMessage> MultiGet()
        {
            await DocumentStore.AsyncDatabaseCommands.MultiGetAsync(new[] { new GetRequest() });
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/nextIdentityFor")]
        public async Task<HttpResponseMessage> NextIdentityFor()
        {
            await DocumentStore.AsyncDatabaseCommands.NextIdentityForAsync("keys");
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/patch1")]
        public async Task<HttpResponseMessage> Patch1()
        {
            await DocumentStore.AsyncDatabaseCommands.PatchAsync("keys/1", new ScriptedPatchRequest { Script = "this.Name = 'John';" });
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/patch2")]
        public async Task<HttpResponseMessage> Patch2()
        {
            await DocumentStore.AsyncDatabaseCommands.PatchAsync("keys/1", new[]
                                                           {
                                                               new PatchRequest
                                                               {
                                                                   Name = "Name", 
                                                                   Type = PatchCommandType.Set, 
                                                                   Value = "John"
                                                               }
                                                           });
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/put")]
        public async Task<HttpResponseMessage> Put()
        {
            await DocumentStore.AsyncDatabaseCommands.PutAsync("keys/1", null, new RavenJObject(), new RavenJObject());
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/putAttachment")]
        public async Task<HttpResponseMessage> PutAttachment()
        {
#pragma warning disable 618
            await DocumentStore.AsyncDatabaseCommands.PutAttachmentAsync("keys/1", null, new MemoryStream(), new RavenJObject());
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/putIndex")]
        public async Task<HttpResponseMessage> PutIndex()
        {
            await DocumentStore.AsyncDatabaseCommands.PutIndexAsync("index1", new IndexDefinition { Map = "from doc in docs select new { doc.Name };" });
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/putTransformer")]
        public async Task<HttpResponseMessage> PutTransformer()
        {
            await DocumentStore.AsyncDatabaseCommands.PutTransformerAsync("transformer1", new TransformerDefinition { Name = "transformer1", TransformResults = "from result in results select result;" });
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/query")]
        public async Task<HttpResponseMessage> Query()
        {
            await DocumentStore.AsyncDatabaseCommands.QueryAsync(new RavenDocumentsByEntityName().IndexName, new IndexQuery());
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/resetIndex")]
        public async Task<HttpResponseMessage> ResetIndex()
        {
            await DocumentStore.AsyncDatabaseCommands.ResetIndexAsync(new RavenDocumentsByEntityName().IndexName);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/seedIdentityFor")]
        public async Task<HttpResponseMessage> SeedIdentityFor()
        {
            await DocumentStore.AsyncDatabaseCommands.SeedIdentityForAsync("keys", 6);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/setIndexLock")]
        public async Task<HttpResponseMessage> SetIndexLock()
        {
            await DocumentStore.AsyncDatabaseCommands.SetIndexLockAsync(new RavenDocumentsByEntityName().IndexName, IndexLockMode.Unlock);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/setIndexPriority")]
        public async Task<HttpResponseMessage> SetIndexPriority()
        {
            await DocumentStore.AsyncDatabaseCommands.SetIndexPriorityAsync(new RavenDocumentsByEntityName().IndexName, IndexingPriority.Normal);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/startsWith")]
        public async Task<HttpResponseMessage> StartsWith()
        {
            await DocumentStore.AsyncDatabaseCommands.StartsWithAsync("keys", null, 0, 128);
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/streamDocs1")]
        public async Task<HttpResponseMessage> StreamDocs1()
        {
            var enumerator = await DocumentStore.AsyncDatabaseCommands.StreamDocsAsync(Etag.Empty);
            while (await enumerator.MoveNextAsync())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/async/commands/streamDocs2")]
        public async Task<HttpResponseMessage> StreamDocs2()
        {
            var enumerator = await DocumentStore.AsyncDatabaseCommands.StreamDocsAsync(startsWith: "keys");
            while (await enumerator.MoveNextAsync())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/async/commands/streamQuery")]
        public async Task<HttpResponseMessage> StreamQuery()
        {
            var queryHeaderInfo = new Reference<QueryHeaderInformation>();
            var enumerator = await DocumentStore.AsyncDatabaseCommands.StreamQueryAsync(new RavenDocumentsByEntityName().IndexName, new IndexQuery(), queryHeaderInfo);
            while (await enumerator.MoveNextAsync())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/async/commands/suggest")]
        public async Task<HttpResponseMessage> Suggest()
        {
            await DocumentStore.AsyncDatabaseCommands.SuggestAsync(new Users_ByName().IndexName, new SuggestionQuery { Field = "Name", Term = "Term1" });
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/updateAttachmentMetadata")]
        public async Task<HttpResponseMessage> UpdateAttachmentMetadata()
        {
#pragma warning disable 618
            await DocumentStore.AsyncDatabaseCommands.UpdateAttachmentMetadataAsync("keys/1", null, new RavenJObject());
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/updateByIndex")]
        public async Task<HttpResponseMessage> UpdateByIndex()
        {
            var operation = await DocumentStore.AsyncDatabaseCommands.UpdateByIndexAsync(new RavenDocumentsByEntityName().IndexName, new IndexQuery { Query = "Tag:Orders" }, new ScriptedPatchRequest { Script = "this.Name = 'John';" }, new BulkOperationOptions { AllowStale = true });
            await operation.WaitForCompletionAsync();

            return new HttpResponseMessage();
        }

        [Route("api/async/commands/info/getReplicationInfo")]
        public async Task<HttpResponseMessage> GetReplicationInfo()
        {
            try
            {
                await DocumentStore.AsyncDatabaseCommands.Info.GetReplicationInfoAsync();
            }
            catch (Exception e)
            {
                if (e.Message.Contains("replication bundle not activated") == false)
                    throw;
            }

            return new HttpResponseMessage();
        }

        [Route("api/async/commands/admin/getDatabaseConfiguration")]
        public async Task<HttpResponseMessage> GetDatabaseConfiguration()
        {
            await DocumentStore.AsyncDatabaseCommands.Admin.GetDatabaseConfigurationAsync();
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/admin/getIndexingStatus")]
        public async Task<HttpResponseMessage> GetIndexingStatus()
        {
            await DocumentStore.AsyncDatabaseCommands.Admin.GetIndexingStatusAsync();
            return new HttpResponseMessage();
        }

        [Route("api/async/commands/admin/startIndexing")]
        public async Task<HttpResponseMessage> StartIndexing()
        {
            try
            {
                await DocumentStore.AsyncDatabaseCommands.Admin.StartIndexingAsync();
            }
            catch (Exception e)
            {
                var message = e.ToString();
                if (message.Contains("The background workers has already been spun and cannot be spun again") == false)
                    throw;
            }

            return new HttpResponseMessage();
        }

        [Route("api/async/commands/admin/stopIndexing")]
        public async Task<HttpResponseMessage> StopIndexing()
        {
            Exception exception = null;
            try
            {
                await DocumentStore.AsyncDatabaseCommands.Admin.StopIndexingAsync();
            }
            catch (Exception e)
            {
                exception = e;
            }

            await DocumentStore.AsyncDatabaseCommands.Admin.StartIndexingAsync();

            if (exception != null)
                throw exception;

            return new HttpResponseMessage();
        }
    }
}
