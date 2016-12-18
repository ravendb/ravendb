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
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Web.Models.Indexes;

namespace Raven.Tests.Web.Controllers.Commands
{
    public class MixedCommandsController : RavenApiController
    {
        [Route("api/mixed/commands/batch")]
        public Task<HttpResponseMessage> Batch()
        {
            DocumentStore.DatabaseCommands.Batch(new List<ICommandData>
                                                 {
                                                     new PutCommandData
                                                     {
                                                         Document = new RavenJObject(), 
                                                         Key = Guid.NewGuid().ToString(), 
                                                         Metadata = new RavenJObject()
                                                     }
                                                 });

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/delete")]
        public Task<HttpResponseMessage> Delete()
        {
            DocumentStore.DatabaseCommands.Delete("keys/1", null);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/deleteAttachment")]
        public Task<HttpResponseMessage> DeleteAttachment()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.DeleteAttachment("keys/1", null);
#pragma warning restore 618
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/deleteByIndex")]
        public Task<HttpResponseMessage> DeleteByIndex()
        {
            var operation = DocumentStore.DatabaseCommands.DeleteByIndex(new RavenDocumentsByEntityName().IndexName, new IndexQuery { Query = "Tag:Orders" }, new BulkOperationOptions { AllowStale = true });
            operation.WaitForCompletion();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/deleteIndex")]
        public Task<HttpResponseMessage> DeleteIndex()
        {
            DocumentStore.DatabaseCommands.DeleteIndex("index1");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/deleteTransformer")]
        public Task<HttpResponseMessage> DeleteTransformer()
        {
            DocumentStore.DatabaseCommands.DeleteTransformer("transformer1");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/get")]
        public Task<HttpResponseMessage> Get()
        {
            DocumentStore.DatabaseCommands.Get("keys/1");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/get2")]
        public Task<HttpResponseMessage> Get2()
        {
            DocumentStore.DatabaseCommands.Get(new[] { "keys/1", "keys/2" }, null);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getAttachment")]
        public Task<HttpResponseMessage> GetAttachment()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.GetAttachment("attachment1");
#pragma warning restore 618
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getAttachmentHeadersStartingWith")]
        public Task<HttpResponseMessage> GetAttachmentHeadersStartingWith()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.GetAttachmentHeadersStartingWith("attachments", 0, 128);
#pragma warning restore 618
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getAttachments")]
        public Task<HttpResponseMessage> GetAttachments()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.GetAttachments(0, Etag.Empty, 128);
#pragma warning restore 618
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getBulkInsertOperation")]
        public Task<HttpResponseMessage> GetBulkInsertOperation()
        {
            using (var operation = DocumentStore.DatabaseCommands.GetBulkInsertOperation(new BulkInsertOptions(), DocumentStore.Changes()))
            {
                operation.Write(Guid.NewGuid().ToString(), new RavenJObject(), new RavenJObject());
            }

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getDocuments1")]
        public Task<HttpResponseMessage> GetDocuments()
        {
            DocumentStore.DatabaseCommands.GetDocuments(Etag.Empty, 10);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getDocuments2")]
        public Task<HttpResponseMessage> GetDocuments2()
        {
            DocumentStore.DatabaseCommands.GetDocuments(0, 10);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getFacets")]
        public Task<HttpResponseMessage> GetFacets()
        {
            DocumentStore.DatabaseCommands.GetFacets(new RavenDocumentsByEntityName().IndexName, new IndexQuery(), new List<Facet> { new Facet { Name = "Facet1", Mode = FacetMode.Default } });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getIndex")]
        public Task<HttpResponseMessage> GetIndex()
        {
            DocumentStore.DatabaseCommands.GetIndex("index1");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getIndexMergeSuggestions")]
        public Task<HttpResponseMessage> GetIndexMergeSuggestions()
        {
            DocumentStore.DatabaseCommands.GetIndexMergeSuggestions();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getIndexNames")]
        public Task<HttpResponseMessage> GetIndexNames()
        {
            DocumentStore.DatabaseCommands.GetIndexNames(0, 10);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getIndexes")]
        public Task<HttpResponseMessage> GetIndexes()
        {
            DocumentStore.DatabaseCommands.GetIndexes(0, 10);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getLicenseStatus")]
        public Task<HttpResponseMessage> GetLicenseStatus()
        {
            DocumentStore.DatabaseCommands.GetLicenseStatus();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getLogs")]
        public Task<HttpResponseMessage> GetLogs()
        {
            DocumentStore.DatabaseCommands.GetLogs(false);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getMultiFacets")]
        public Task<HttpResponseMessage> GetMultiFacets()
        {
            DocumentStore.DatabaseCommands.GetMultiFacets(new[]
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
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getStatistics")]
        public Task<HttpResponseMessage> GetStatistics()
        {
            DocumentStore.DatabaseCommands.GetStatistics();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getTerms")]
        public Task<HttpResponseMessage> GetTerms()
        {
            DocumentStore.DatabaseCommands.GetTerms(new RavenDocumentsByEntityName().IndexName, "Tag", string.Empty, 128);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getTransformer")]
        public Task<HttpResponseMessage> GetTransformer()
        {
            DocumentStore.DatabaseCommands.GetTransformer("transformer1");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/getTransformers")]
        public Task<HttpResponseMessage> GetTransformers()
        {
            DocumentStore.DatabaseCommands.GetTransformers(0, 128);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/head")]
        public Task<HttpResponseMessage> Head()
        {
            DocumentStore.DatabaseCommands.Head("keys/1");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/headAttachment")]
        public Task<HttpResponseMessage> HeadAttachment()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.HeadAttachment("keys/1");
#pragma warning restore 618
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/indexHasChanged")]
        public Task<HttpResponseMessage> IndexHasChanged()
        {
            DocumentStore.DatabaseCommands.IndexHasChanged(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/moreLikeThis")]
        public Task<HttpResponseMessage> MoreLikeThis()
        {
            var key = Guid.NewGuid().ToString();
            DocumentStore.DatabaseCommands.GetStatistics();
            DocumentStore.DatabaseCommands.Put(key, null, new RavenJObject(), new RavenJObject());

            SpinWait.SpinUntil(() => DocumentStore.DatabaseCommands.GetStatistics().StaleIndexes.Length == 0);

            DocumentStore.DatabaseCommands.MoreLikeThis(new MoreLikeThisQuery { IndexName = new RavenDocumentsByEntityName().IndexName, DocumentId = key });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/multiGet")]
        public Task<HttpResponseMessage> MultiGet()
        {
            DocumentStore.DatabaseCommands.MultiGet(new[] { new GetRequest() });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/nextIdentityFor")]
        public Task<HttpResponseMessage> NextIdentityFor()
        {
            DocumentStore.DatabaseCommands.NextIdentityFor("keys");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/patch1")]
        public Task<HttpResponseMessage> Patch1()
        {
            DocumentStore.DatabaseCommands.Patch("keys/1", new ScriptedPatchRequest { Script = "this.Name = 'John';" });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/patch2")]
        public Task<HttpResponseMessage> Patch2()
        {
            DocumentStore.DatabaseCommands.Patch("keys/1", new[]
                                                           {
                                                               new PatchRequest
                                                               {
                                                                   Name = "Name", 
                                                                   Type = PatchCommandType.Set, 
                                                                   Value = "John"
                                                               }
                                                           });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/put")]
        public Task<HttpResponseMessage> Put()
        {
            DocumentStore.DatabaseCommands.Put("keys/1", null, new RavenJObject(), new RavenJObject());
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/putAttachment")]
        public Task<HttpResponseMessage> PutAttachment()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.PutAttachment("keys/1", null, new MemoryStream(), new RavenJObject());
#pragma warning restore 618
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/putIndex")]
        public Task<HttpResponseMessage> PutIndex()
        {
            DocumentStore.DatabaseCommands.PutIndex("index1", new IndexDefinition { Map = "from doc in docs select new { doc.Name };" });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/putTransformer")]
        public Task<HttpResponseMessage> PutTransformer()
        {
            DocumentStore.DatabaseCommands.PutTransformer("transformer1", new TransformerDefinition { Name = "transformer1", TransformResults = "from result in results select result;" });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/query")]
        public Task<HttpResponseMessage> Query()
        {
            DocumentStore.DatabaseCommands.Query(new RavenDocumentsByEntityName().IndexName, new IndexQuery());
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/resetIndex")]
        public Task<HttpResponseMessage> ResetIndex()
        {
            DocumentStore.DatabaseCommands.ResetIndex(new RavenDocumentsByEntityName().IndexName);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/seedIdentityFor")]
        public Task<HttpResponseMessage> SeedIdentityFor()
        {
            DocumentStore.DatabaseCommands.SeedIdentityFor("keys", 6);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/setIndexLock")]
        public Task<HttpResponseMessage> SetIndexLock()
        {
            DocumentStore.DatabaseCommands.SetIndexLock(new RavenDocumentsByEntityName().IndexName, IndexLockMode.Unlock);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/setIndexPriority")]
        public Task<HttpResponseMessage> SetIndexPriority()
        {
            DocumentStore.DatabaseCommands.SetIndexPriority(new RavenDocumentsByEntityName().IndexName, IndexingPriority.Normal);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/startsWith")]
        public Task<HttpResponseMessage> StartsWith()
        {
            DocumentStore.DatabaseCommands.StartsWith("keys", null, 0, 128);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/streamDocs1")]
        public Task<HttpResponseMessage> StreamDocs1()
        {
            var enumerator = DocumentStore.DatabaseCommands.StreamDocs(Etag.Empty);
            while (enumerator.MoveNext())
            {
            }

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/streamDocs2")]
        public Task<HttpResponseMessage> StreamDocs2()
        {
            var enumerator = DocumentStore.DatabaseCommands.StreamDocs(startsWith: "keys");
            while (enumerator.MoveNext())
            {
            }

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/streamQuery")]
        public Task<HttpResponseMessage> StreamQuery()
        {
            QueryHeaderInformation queryHeaderInfo;
            var enumerator = DocumentStore.DatabaseCommands.StreamQuery(new RavenDocumentsByEntityName().IndexName, new IndexQuery(), out queryHeaderInfo);
            while (enumerator.MoveNext())
            {
            }

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/suggest")]
        public Task<HttpResponseMessage> Suggest()
        {
            DocumentStore.DatabaseCommands.Suggest(new Users_ByName().IndexName, new SuggestionQuery { Field = "Name", Term = "Term1" });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/updateAttachmentMetadata")]
        public Task<HttpResponseMessage> UpdateAttachmentMetadata()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.UpdateAttachmentMetadata("keys/1", null, new RavenJObject());
#pragma warning restore 618
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/updateByIndex")]
        public Task<HttpResponseMessage> UpdateByIndex()
        {
            var operation = DocumentStore.DatabaseCommands.UpdateByIndex(new RavenDocumentsByEntityName().IndexName, new IndexQuery { Query = "Tag:Orders" }, new ScriptedPatchRequest { Script = "this.Name = 'John';" }, new BulkOperationOptions { AllowStale = true });
            operation.WaitForCompletion();

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/info/getReplicationInfo")]
        public Task<HttpResponseMessage> GetReplicationInfo()
        {
            try
            {
                DocumentStore.DatabaseCommands.Info.GetReplicationInfo();
            }
            catch (Exception e)
            {
                if (e.Message.Contains("replication bundle not activated") == false)
                    throw;
            }

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/admin/getDatabaseConfiguration")]
        public Task<HttpResponseMessage> GetDatabaseConfiguration()
        {
            DocumentStore.DatabaseCommands.Admin.GetDatabaseConfiguration();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/admin/getIndexingStatus")]
        public Task<HttpResponseMessage> GetIndexingStatus()
        {
            DocumentStore.DatabaseCommands.Admin.GetIndexingStatus();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/admin/startIndexing")]
        public Task<HttpResponseMessage> StartIndexing()
        {
            try
            {
                DocumentStore.DatabaseCommands.Admin.StartIndexing();
            }
            catch (Exception e)
            {
                if (e.Message.Contains("The background workers has already been spun and cannot be spun again") == false)
                    throw;
            }

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/commands/admin/stopIndexing")]
        public Task<HttpResponseMessage> StopIndexing()
        {
            try
            {
                DocumentStore.DatabaseCommands.Admin.StopIndexing();
            }
            finally
            {
                DocumentStore.DatabaseCommands.Admin.StartIndexing();
            }

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }
    }
}
