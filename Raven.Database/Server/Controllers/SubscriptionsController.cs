// -----------------------------------------------------------------------
//  <copyright file="SubscriptionsController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Abstractions.Json;
using Raven.Database.Actions;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public class SubscriptionsController : RavenDbApiController
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        [HttpPost]
        [RavenRoute("subscriptions/create")]
        [RavenRoute("databases/{databaseName}/subscriptions/create")]
        public async Task<HttpResponseMessage> Create()
        {
            var subscriptionCriteria = await ReadJsonObjectAsync<SubscriptionCriteria>();

            if(subscriptionCriteria == null)
                throw new InvalidOperationException("Criteria cannot be null");

            var id = Database.Subscriptions.CreateSubscription(subscriptionCriteria);

            return GetMessageWithObject(new
            {
                Id = id
            }, HttpStatusCode.Created);
        }

        [HttpDelete]
        [RavenRoute("subscriptions")]
        [RavenRoute("databases/{databaseName}/subscriptions")]
        public HttpResponseMessage Delete(long id)
        {
            Database.Subscriptions.DeleteSubscription(id);

            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("subscriptions/open")]
        [RavenRoute("databases/{databaseName}/subscriptions/open")]
        public async Task<HttpResponseMessage> Open(long id)
        {
            Database.Subscriptions.GetSubscriptionConfig(id);

            var options = await ReadJsonObjectAsync<SubscriptionConnectionOptions>();

            if (options == null)
                throw new InvalidOperationException("Options cannot be null");

            Database.Subscriptions.OpenSubscription(id, options);

            Database.Notifications.RaiseNotifications(new DataSubscriptionChangeNotification
            {
                Id = id,
                Type = DataSubscriptionChangeTypes.SubscriptionOpened
            });

            return GetEmptyMessage();
        }

        [HttpGet]
        [RavenRoute("subscriptions/pull")]
        [RavenRoute("databases/{databaseName}/subscriptions/pull")]
        public HttpResponseMessage Pull(long id, string connection)
        {
            Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);

            var pushStreamContent = new PushStreamContent((stream, content, transportContext) => StreamToClient(id, Database.Subscriptions, stream))
            {
                Headers =
                {
                    ContentType = new MediaTypeHeaderValue("application/json")
                    {
                        CharSet = "utf-8"
                    }
                }
            };

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = pushStreamContent
            };
        }

        [HttpPost]
        [RavenRoute("subscriptions/acknowledgeBatch")]
        [RavenRoute("databases/{databaseName}/subscriptions/acknowledgeBatch")]
        public HttpResponseMessage AcknowledgeBatch(long id, string lastEtag, string connection)
        {
            Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);

            try
            {
                Database.Subscriptions.AcknowledgeBatchProcessed(id, Etag.Parse(lastEtag));
            }
            catch (TimeoutException)
            {
                return GetMessageWithString("The subscription cannot be acknowledged because the timeout has been reached.", HttpStatusCode.RequestTimeout);
            }

            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("subscriptions/close")]
        [RavenRoute("databases/{databaseName}/subscriptions/close")]
        public HttpResponseMessage Close(long id, string connection, bool force = false)
        {
            if (force == false)
            {
                try
                {
                    Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);
                }
                catch (SubscriptionException)
                {
                    // ignore if assertion exception happened on close
                    return GetEmptyMessage();
                }
            }

            Database.Subscriptions.ReleaseSubscription(id, force);

            Database.Notifications.RaiseNotifications(new DataSubscriptionChangeNotification
            {
                Id = id,
                Type = DataSubscriptionChangeTypes.SubscriptionReleased
            });

            return GetEmptyMessage();
        }

        [HttpPatch]
        [RavenRoute("subscriptions/client-alive")]
        [RavenRoute("databases/{databaseName}/subscriptions/client-alive")]
        public HttpResponseMessage ClientAlive(long id, string connection)
        {
            Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);

            Database.Subscriptions.UpdateClientActivityDate(id);

            return GetEmptyMessage();
        }

        [HttpGet]
        [RavenRoute("subscriptions")]
        [RavenRoute("databases/{databaseName}/subscriptions")]
        public HttpResponseMessage Get()
        {
            var start = GetStart();
            var take = GetPageSize(Database.Configuration.MaxPageSize);

            return GetMessageWithObject(Database.Subscriptions.GetSubscriptions(start, take));
        }

        private void StreamToClient(long id, SubscriptionActions subscriptions, Stream stream)
        {
            var sentDocuments = false;

            var bufferStream = new BufferedStream(stream, 1024 * 64);
            using (var writer = new JsonTextWriter(new StreamWriter(bufferStream)))
            {
                var options = subscriptions.GetBatchOptions(id);

                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                writer.WriteStartArray();

                using (var cts = new CancellationTokenSource())
                using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
                {
                    Etag lastProcessedDocEtag = null;

                    var batchSize = 0;
                    var batchDocCount = 0;
                    var processedDocuments = 0;
                    var hasMoreDocs = false;
                    var config = subscriptions.GetSubscriptionConfig(id);
                    var startEtag =  config.AckEtag;
                    var criteria = config.Criteria;

                    bool isPrefixCriteria = !string.IsNullOrWhiteSpace(criteria.KeyStartsWith);

                    Func<JsonDocument, bool> addDocument = doc =>
                    {
                        processedDocuments++;
                        timeout.Delay();

                        // We cant continue because we have already maxed out the batch bytes size.
                        if (options.MaxSize.HasValue && batchSize >= options.MaxSize)
                            return false;

                        // We cant continue because we have already maxed out the amount of documents to send.
                        if (batchDocCount >= options.MaxDocCount)
                            return false;

                        // We can continue because we are ignoring system documents.
                        if (doc.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
                            return true;

                        // We can continue because we are ignoring the document as it doesn't fit the criteria.
                        if (MatchCriteria(criteria, doc) == false)
                            return true;

                        doc.ToJson().WriteTo(writer);
                        writer.WriteRaw(Environment.NewLine);

                        batchSize += doc.SerializedSizeOnDisk;
                        batchDocCount++;

                        return true; // We get the next document
                    };

                    int retries = 0;
                    do
                    {
                        int lastIndex = processedDocuments;

                        Database.TransactionalStorage.Batch(accessor =>
                        {
                            // we may be sending a LOT of documents to the user, and most 
                            // of them aren't going to be relevant for other ops, so we are going to skip
                            // the cache for that, to avoid filling it up very quickly
                            using (DocumentCacher.SkipSetAndGetDocumentsInDocumentCache())
                            {    
                                if (isPrefixCriteria)
                                {
                                    // If we don't get any document from GetDocumentsWithIdStartingWith it could be that we are in presence of a lagoon of uninteresting documents, so we are hitting a timeout.
                                    lastProcessedDocEtag = Database.Documents.GetDocumentsWithIdStartingWith(criteria.KeyStartsWith, options.MaxDocCount - batchDocCount, startEtag, cts.Token, addDocument);

                                    hasMoreDocs = false;
                                }
                                else
                                {
                                    // It doesn't matter if we match the criteria or not, the document has been already processed.
                                    lastProcessedDocEtag = Database.Documents.GetDocuments(-1, options.MaxDocCount - batchDocCount, startEtag, cts.Token, addDocument);

                                    // If we don't get any document from GetDocuments it may be a signal that something is wrong.
                                    if (lastProcessedDocEtag == null)
                                    {
                                        hasMoreDocs = false;
                                    }
                                    else
                                    {
                                        var lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
                                        hasMoreDocs = EtagUtil.IsGreaterThan(lastDocEtag, lastProcessedDocEtag);

                                        startEtag = lastProcessedDocEtag;
                                    }

                                    retries = lastIndex == batchDocCount ? retries : 0;
                                }
                            }							
                        });

                        if (lastIndex == processedDocuments)
                        {
                            if (retries == 3)
                            {
                                log.Warn("Subscription processing did not end up replicating any documents for 3 times in a row, stopping operation", retries);
                            }
                            else
                            {
                                log.Warn("Subscription processing did not end up replicating any documents, due to possible storage error, retry number: {0}", retries);
                            }
                            retries++;
                        }
                    } 
                    while (retries < 3 && hasMoreDocs && batchDocCount < options.MaxDocCount && (options.MaxSize.HasValue == false || batchSize < options.MaxSize));

                    writer.WriteEndArray();

                    if (batchDocCount > 0 || isPrefixCriteria)
                    {
                        writer.WritePropertyName("LastProcessedEtag");
                        writer.WriteValue(lastProcessedDocEtag.ToString());

                        sentDocuments = true;
                    }

                    writer.WriteEndObject();
                    writer.Flush();

                    bufferStream.Flush();
                }
            }

            if (sentDocuments)
                subscriptions.UpdateBatchSentTime(id);
        }

        private static bool MatchCriteria(SubscriptionCriteria criteria, JsonDocument doc)
        {
            if (criteria.BelongsToAnyCollection != null &&
                criteria.BelongsToAnyCollection.Contains(doc.Metadata.Value<string>(Constants.RavenEntityName), StringComparer.InvariantCultureIgnoreCase) == false)
                return false;

            if (criteria.KeyStartsWith != null && doc.Key.StartsWith(criteria.KeyStartsWith) == false)
                return false;

            if (criteria.PropertiesMatch != null)
            {
                foreach (var match in criteria.PropertiesMatch)
                {
                    var tokens = doc.DataAsJson.SelectTokenWithRavenSyntaxReturningFlatStructure(match.Key).Select(x => x.Item1).ToArray();
                                    
                    foreach (var curVal in tokens)
                    {
                        if (RavenJToken.DeepEquals(curVal, match.Value) == false)
                            return false;
                    }

                    if (tokens.Length == 0)
                        return false;					
                }
            }

            if (criteria.PropertiesNotMatch != null)
            {
                foreach (var match in criteria.PropertiesNotMatch)
                {
                    var tokens = doc.DataAsJson.SelectTokenWithRavenSyntaxReturningFlatStructure(match.Key).Select(x => x.Item1).ToArray();
                                        
                    foreach (var curVal in tokens)
                    {
                        if (RavenJToken.DeepEquals(curVal, match.Value) == true)
                            return false;
                    }					
                }				
            }

            return true;
        }
    }
}
