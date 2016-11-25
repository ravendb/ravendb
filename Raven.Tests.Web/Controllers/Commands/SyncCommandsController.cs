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
using System.Web.Http;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Web.Models.Indexes;

namespace Raven.Tests.Web.Controllers.Commands
{
    public class SyncCommandsController : RavenApiController
    {
        [Route("api/sync/commands/batch")]
        public HttpResponseMessage Batch()
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
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/delete")]
        public HttpResponseMessage Delete()
        {
            DocumentStore.DatabaseCommands.Delete("keys/1", null);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/deleteAttachment")]
        public HttpResponseMessage DeleteAttachment()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.DeleteAttachment("keys/1", null);
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/deleteByIndex")]
        public HttpResponseMessage DeleteByIndex()
        {
            var operation = DocumentStore.DatabaseCommands.DeleteByIndex(new RavenDocumentsByEntityName().IndexName, new IndexQuery { Query = "Tag:Orders" }, new BulkOperationOptions { AllowStale = true });
            operation.WaitForCompletion();
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/deleteIndex")]
        public HttpResponseMessage DeleteIndex()
        {
            DocumentStore.DatabaseCommands.DeleteIndex("index1");
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/deleteTransformer")]
        public HttpResponseMessage DeleteTransformer()
        {
            DocumentStore.DatabaseCommands.DeleteTransformer("transformer1");
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/get")]
        public HttpResponseMessage Get()
        {
            DocumentStore.DatabaseCommands.Get("keys/1");
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/get2")]
        public HttpResponseMessage Get2()
        {
            DocumentStore.DatabaseCommands.Get(new[] { "keys/1", "keys/2" }, null);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getAttachment")]
        public HttpResponseMessage GetAttachment()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.GetAttachment("attachment1");
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getAttachmentHeadersStartingWith")]
        public HttpResponseMessage GetAttachmentHeadersStartingWith()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.GetAttachmentHeadersStartingWith("attachments", 0, 128);
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getAttachments")]
        public HttpResponseMessage GetAttachments()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.GetAttachments(0, Etag.Empty, 128);
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getBulkInsertOperation")]
        public HttpResponseMessage GetBulkInsertOperation()
        {
            using (var operation = DocumentStore.DatabaseCommands.GetBulkInsertOperation(new BulkInsertOptions(), DocumentStore.Changes()))
            {
                operation.Write(Guid.NewGuid().ToString(), new RavenJObject(), new RavenJObject());
            }

            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getDocuments1")]
        public HttpResponseMessage GetDocuments()
        {
            DocumentStore.DatabaseCommands.GetDocuments(Etag.Empty, 10);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getDocuments2")]
        public HttpResponseMessage GetDocuments2()
        {
            DocumentStore.DatabaseCommands.GetDocuments(0, 10);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getFacets")]
        public HttpResponseMessage GetFacets()
        {
            DocumentStore.DatabaseCommands.GetFacets(new RavenDocumentsByEntityName().IndexName, new IndexQuery(), new List<Facet> { new Facet { Name = "Facet1", Mode = FacetMode.Default } });
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getIndex")]
        public HttpResponseMessage GetIndex()
        {
            DocumentStore.DatabaseCommands.GetIndex("index1");
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getIndexMergeSuggestions")]
        public HttpResponseMessage GetIndexMergeSuggestions()
        {
            DocumentStore.DatabaseCommands.GetIndexMergeSuggestions();
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getIndexNames")]
        public HttpResponseMessage GetIndexNames()
        {
            DocumentStore.DatabaseCommands.GetIndexNames(0, 10);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getIndexes")]
        public HttpResponseMessage GetIndexes()
        {
            DocumentStore.DatabaseCommands.GetIndexes(0, 10);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getLicenseStatus")]
        public HttpResponseMessage GetLicenseStatus()
        {
            DocumentStore.DatabaseCommands.GetLicenseStatus();
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getLogs")]
        public HttpResponseMessage GetLogs()
        {
            DocumentStore.DatabaseCommands.GetLogs(false);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getMultiFacets")]
        public HttpResponseMessage GetMultiFacets()
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
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getStatistics")]
        public HttpResponseMessage GetStatistics()
        {
            DocumentStore.DatabaseCommands.GetStatistics();
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getTerms")]
        public HttpResponseMessage GetTerms()
        {
            DocumentStore.DatabaseCommands.GetTerms(new RavenDocumentsByEntityName().IndexName, "Tag", string.Empty, 128);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getTransformer")]
        public HttpResponseMessage GetTransformer()
        {
            DocumentStore.DatabaseCommands.GetTransformer("transformer1");
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/getTransformers")]
        public HttpResponseMessage GetTransformers()
        {
            DocumentStore.DatabaseCommands.GetTransformers(0, 128);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/head")]
        public HttpResponseMessage Head()
        {
            DocumentStore.DatabaseCommands.Head("keys/1");
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/headAttachment")]
        public HttpResponseMessage HeadAttachment()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.HeadAttachment("keys/1");
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/indexHasChanged")]
        public HttpResponseMessage IndexHasChanged()
        {
            DocumentStore.DatabaseCommands.IndexHasChanged(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/moreLikeThis")]
        public HttpResponseMessage MoreLikeThis()
        {
            var key = Guid.NewGuid().ToString();
            DocumentStore.DatabaseCommands.GetStatistics();
            DocumentStore.DatabaseCommands.Put(key, null, new RavenJObject(), new RavenJObject());

            SpinWait.SpinUntil(() => DocumentStore.DatabaseCommands.GetStatistics().StaleIndexes.Length == 0);

            DocumentStore.DatabaseCommands.MoreLikeThis(new MoreLikeThisQuery { IndexName = new RavenDocumentsByEntityName().IndexName, DocumentId = key });
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/multiGet")]
        public HttpResponseMessage MultiGet()
        {
            DocumentStore.DatabaseCommands.MultiGet(new[] { new GetRequest() });
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/nextIdentityFor")]
        public HttpResponseMessage NextIdentityFor()
        {
            DocumentStore.DatabaseCommands.NextIdentityFor("keys");
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/patch1")]
        public HttpResponseMessage Patch1()
        {
            DocumentStore.DatabaseCommands.Patch("keys/1", new ScriptedPatchRequest { Script = "this.Name = 'John';" });
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/patch2")]
        public HttpResponseMessage Patch2()
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
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/put")]
        public HttpResponseMessage Put()
        {
            DocumentStore.DatabaseCommands.Put("keys/1", null, new RavenJObject(), new RavenJObject());
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/putAttachment")]
        public HttpResponseMessage PutAttachment()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.PutAttachment("keys/1", null, new MemoryStream(), new RavenJObject());
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/putIndex")]
        public HttpResponseMessage PutIndex()
        {
            DocumentStore.DatabaseCommands.PutIndex("index1", new IndexDefinition { Map = "from doc in docs select new { doc.Name };" });
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/putTransformer")]
        public HttpResponseMessage PutTransformer()
        {
            DocumentStore.DatabaseCommands.PutTransformer("transformer1", new TransformerDefinition { Name = "transformer1", TransformResults = "from result in results select result;" });
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/query")]
        public HttpResponseMessage Query()
        {
            DocumentStore.DatabaseCommands.Query(new RavenDocumentsByEntityName().IndexName, new IndexQuery());
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/resetIndex")]
        public HttpResponseMessage ResetIndex()
        {
            DocumentStore.DatabaseCommands.ResetIndex(new RavenDocumentsByEntityName().IndexName);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/seedIdentityFor")]
        public HttpResponseMessage SeedIdentityFor()
        {
            DocumentStore.DatabaseCommands.SeedIdentityFor("keys", 6);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/setIndexLock")]
        public HttpResponseMessage SetIndexLock()
        {
            DocumentStore.DatabaseCommands.SetIndexLock(new RavenDocumentsByEntityName().IndexName, IndexLockMode.Unlock);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/setIndexPriority")]
        public HttpResponseMessage SetIndexPriority()
        {
            DocumentStore.DatabaseCommands.SetIndexPriority(new RavenDocumentsByEntityName().IndexName, IndexingPriority.Normal);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/startsWith")]
        public HttpResponseMessage StartsWith()
        {
            DocumentStore.DatabaseCommands.StartsWith("keys", null, 0, 128);
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/streamDocs1")]
        public HttpResponseMessage StreamDocs1()
        {
            var enumerator = DocumentStore.DatabaseCommands.StreamDocs(Etag.Empty);
            while (enumerator.MoveNext())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/streamDocs2")]
        public HttpResponseMessage StreamDocs2()
        {
            var enumerator = DocumentStore.DatabaseCommands.StreamDocs(startsWith: "keys");
            while (enumerator.MoveNext())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/streamQuery")]
        public HttpResponseMessage StreamQuery()
        {
            QueryHeaderInformation queryHeaderInfo;
            var enumerator = DocumentStore.DatabaseCommands.StreamQuery(new RavenDocumentsByEntityName().IndexName, new IndexQuery(), out queryHeaderInfo);
            while (enumerator.MoveNext())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/suggest")]
        public HttpResponseMessage Suggest()
        {
            DocumentStore.DatabaseCommands.Suggest(new Users_ByName().IndexName, new SuggestionQuery { Field = "Name", Term = "Term1" });
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/updateAttachmentMetadata")]
        public HttpResponseMessage UpdateAttachmentMetadata()
        {
#pragma warning disable 618
            DocumentStore.DatabaseCommands.UpdateAttachmentMetadata("keys/1", null, new RavenJObject());
#pragma warning restore 618
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/updateByIndex")]
        public HttpResponseMessage UpdateByIndex()
        {
            var operation = DocumentStore.DatabaseCommands.UpdateByIndex(new RavenDocumentsByEntityName().IndexName, new IndexQuery { Query = "Tag:Orders" }, new ScriptedPatchRequest { Script = "this.Name = 'John';" }, new BulkOperationOptions { AllowStale = true });
            operation.WaitForCompletion();

            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/info/getReplicationInfo")]
        public HttpResponseMessage GetReplicationInfo()
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

            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/admin/getDatabaseConfiguration")]
        public HttpResponseMessage GetDatabaseConfiguration()
        {
            DocumentStore.DatabaseCommands.Admin.GetDatabaseConfiguration();
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/admin/getIndexingStatus")]
        public HttpResponseMessage GetIndexingStatus()
        {
            DocumentStore.DatabaseCommands.Admin.GetIndexingStatus();
            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/admin/startIndexing")]
        public HttpResponseMessage StartIndexing()
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

            return new HttpResponseMessage();
        }

        [Route("api/sync/commands/admin/stopIndexing")]
        public HttpResponseMessage StopIndexing()
        {
            try
            {
                DocumentStore.DatabaseCommands.Admin.StopIndexing();
            }
            finally
            {
                DocumentStore.DatabaseCommands.Admin.StartIndexing();
            }

            return new HttpResponseMessage();
        }
    }
}
