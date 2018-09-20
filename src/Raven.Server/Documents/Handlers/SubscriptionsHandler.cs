using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class SubscriptionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/subscriptions/try", "POST", AuthorizationStatus.ValidUser)]
        public async Task Try()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                BlittableJsonReaderObject json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                SubscriptionTryout tryout = JsonDeserializationServer.SubscriptionTryout(json);

                SubscriptionConnection.ParsedSubscription sub = SubscriptionConnection.ParseSubscriptionQuery(tryout.Query);
                SubscriptionPatchDocument patch = null;
                if (string.IsNullOrEmpty(sub.Script) == false)
                {
                    patch = new SubscriptionPatchDocument(sub.Script, sub.Functions);
                }

                if (sub.Collection == null)
                    throw new ArgumentException("Collection must be specified");

                var pageSize = GetIntValueQueryString("pageSize") ?? 1;

                var state = new SubscriptionState
                {
                    ChangeVectorForNextBatchStartingPoint = tryout.ChangeVector,
                    Query = tryout.Query
                };

                var fetcher = new SubscriptionDocumentsFetcher(Database, pageSize, -0x42,
                    new IPEndPoint(HttpContext.Connection.RemoteIpAddress, HttpContext.Connection.RemotePort), sub.Collection, sub.Revisions, state, patch);

                var includeCmd = new IncludeDocumentsCommand(Database.DocumentsStorage, context, sub.Includes);

                if (Enum.TryParse(
                    tryout.ChangeVector,
                    out Constants.Documents.SubscriptionChangeVectorSpecialStates changeVectorSpecialValue))
                {
                    switch (changeVectorSpecialValue)
                    {
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.BeginningOfTime:
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange:
                            state.ChangeVectorForNextBatchStartingPoint = null;
                            break;
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument:
                            state.ChangeVectorForNextBatchStartingPoint = Database.DocumentsStorage.GetLastDocumentChangeVector(context, sub.Collection);
                            break;
                    }
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();

                    using (context.OpenReadTransaction())
                    {
                        var first = true;

                        foreach (var itemDetails in fetcher.GetDataToSend(context, includeCmd, 0))
                        {
                            if (itemDetails.Doc.Data == null)
                                continue;

                            includeCmd.Gather(itemDetails.Doc);

                            if (first == false)
                                writer.WriteComma();

                            if (itemDetails.Exception == null)
                            {
                                writer.WriteDocument(context, itemDetails.Doc, metadataOnly: false);
                            }
                            else
                            {
                                var documentWithException = new DocumentWithException
                                {
                                    Exception = itemDetails.Exception.ToString(),
                                    ChangeVector = itemDetails.Doc.ChangeVector,
                                    Id = itemDetails.Doc.Id,
                                    DocumentData = itemDetails.Doc.Data
                                };
                                writer.WriteObject(context.ReadObject(documentWithException.ToJson(), ""));
                            }

                            first = false;
                        }

                        writer.WriteEndArray();
                        writer.WriteComma();
                        writer.WritePropertyName("Includes");
                        var includes = new List<Document>();
                        includeCmd.Fill(includes);
                        writer.WriteIncludes(context, includes);
                        writer.WriteEndObject();
                    }
                }
            }
        }

        [RavenAction("/databases/*/subscriptions", "PUT", AuthorizationStatus.ValidUser)]
        public async Task Create()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var options = JsonDeserializationServer.SubscriptionCreationParams(json);

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(json.ToString(), TrafficWatchChangeType.Subscriptions);

                var sub = SubscriptionConnection.ParseSubscriptionQuery(options.Query);

                if (Enum.TryParse(
                    options.ChangeVector,
                    out Constants.Documents.SubscriptionChangeVectorSpecialStates changeVectorSpecialValue))
                {
                    switch (changeVectorSpecialValue)
                    {
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.BeginningOfTime:

                            options.ChangeVector = null;
                            break;
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument:
                            options.ChangeVector = Database.DocumentsStorage.GetLastDocumentChangeVector(context, sub.Collection);
                            break;
                    }
                }
                var id = GetLongQueryString("id", required: false);
                var disabled = GetBoolValueQueryString("disabled", required: false);
                var mentor = options.MentorNode;
                var subscriptionId = await Database.SubscriptionStorage.PutSubscription(options, id, disabled, mentor: mentor);
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created; // Created

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Name"] = options.Name ?? subscriptionId.ToString()
                    });
                }
            }
        }

        [RavenAction("/databases/*/subscriptions", "DELETE", AuthorizationStatus.ValidUser)]
        public async Task Delete()
        {
            var subscriptionName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("taskName");

            await Database.SubscriptionStorage.DeleteSubscription(subscriptionName);

            await NoContent();
        }

        [RavenAction("/databases/*/subscriptions/state", "GET", AuthorizationStatus.ValidUser)]
        public Task GetSubscriptionState()
        {
            var subscriptionName = GetStringQueryString("name", false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                if (string.IsNullOrEmpty(subscriptionName))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return Task.CompletedTask;
                }

                var subscriptionState = Database
                    .SubscriptionStorage
                    .GetSubscriptionFromServerStore(subscriptionName);


                if (subscriptionState == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                context.Write(writer, subscriptionState.ToJson());

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/subscriptions/connection-details", "GET", AuthorizationStatus.ValidUser)]
        public Task GetSubscriptionConnectionDetails()
        {
            SetupCORSHeaders();

            var subscriptionName = GetStringQueryString("name", false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                if (string.IsNullOrEmpty(subscriptionName))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return Task.CompletedTask;
                }

                var state = Database.SubscriptionStorage.GetSubscriptionConnection(context, subscriptionName);

                var subscriptionConnectionDetails = new SubscriptionConnectionDetails
                {
                    ClientUri = state?.Connection?.ClientUri,
                    Strategy = state?.Connection?.Strategy
                };

                context.Write(writer, subscriptionConnectionDetails.ToJson());
                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/subscriptions", "GET", AuthorizationStatus.ValidUser)]
        public Task GetAll()
        {
            var start = GetStart();
            var pageSize = GetPageSize();
            var history = GetBoolValueQueryString("history", required: false) ?? false;
            var running = GetBoolValueQueryString("running", required: false) ?? false;
            var id = GetLongQueryString("id", required: false);
            var name = GetStringQueryString("name", required: false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                IEnumerable<SubscriptionStorage.SubscriptionGeneralDataAndStats> subscriptions;
                if (string.IsNullOrEmpty(name) && id == null)
                {
                    subscriptions = running
                        ? Database.SubscriptionStorage.GetAllRunningSubscriptions(context, history, start, pageSize)
                        : Database.SubscriptionStorage.GetAllSubscriptions(context, history, start, pageSize);
                }
                else
                {
                    var subscription = running
                        ? Database
                            .SubscriptionStorage
                            .GetRunningSubscription(context, id, name, history)
                        : Database
                            .SubscriptionStorage
                            .GetSubscription(context, id, name, history);

                    if (subscription == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }

                    subscriptions = new[] { subscription };
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {

                    writer.WriteStartObject();

                    var subscriptionsAsBlittable = subscriptions.Select(x => new DynamicJsonValue()
                    {
                        [nameof(SubscriptionState.SubscriptionId)] = x.SubscriptionId,
                        [nameof(SubscriptionState.SubscriptionName)] = x.SubscriptionName,
                        [nameof(SubscriptionState.ChangeVectorForNextBatchStartingPoint)] = x.ChangeVectorForNextBatchStartingPoint,
                        [nameof(SubscriptionState.Query)] = x.Query,
                        [nameof(SubscriptionState.Disabled)] = x.Disabled,
                        [nameof(SubscriptionState.LastClientConnectionTime)] = x.LastClientConnectionTime,
                        [nameof(SubscriptionState.LastBatchAckTime)] = x.LastBatchAckTime,
                        ["Connection"] = GetSubscriptionConnectionDJV(x.Connection),
                        ["RecentConnections"] = x.RecentConnections?.Select(r => new DynamicJsonValue()
                        {
                            ["State"] = new DynamicJsonValue()
                            {
                                ["LatestChangeVectorClientACKnowledged"] = r.SubscriptionState.ChangeVectorForNextBatchStartingPoint,
                                ["Query"] = r.SubscriptionState.Query
                            },
                            ["Connection"] = GetSubscriptionConnectionDJV(r)
                        }),
                        ["FailedConnections"] = x.RecentRejectedConnections?.Select(r => new DynamicJsonValue()
                        {
                            ["State"] = new DynamicJsonValue()
                            {
                                ["LatestChangeVectorClientACKnowledged"] = r.SubscriptionState.ChangeVectorForNextBatchStartingPoint,
                                ["Query"] = r.SubscriptionState.Query
                            },
                            ["Connection"] = GetSubscriptionConnectionDJV(r)
                        }).ToList()
                    });

                    writer.WriteArray(context, "Results", subscriptionsAsBlittable, (w, c, subscription) =>
                    {
                        c.Write(w, subscription);
                    });

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        private static DynamicJsonValue GetSubscriptionConnectionDJV(SubscriptionConnection x)
        {
            if (x == null)
                return new DynamicJsonValue();

            return new DynamicJsonValue()
            {
                [nameof(SubscriptionConnection.ClientUri)] = x.ClientUri,
                [nameof(SubscriptionConnection.Strategy)] = x.Strategy,
                [nameof(SubscriptionConnection.Stats)] = GetConnectionStatsDJV(x.Stats),
                [nameof(SubscriptionConnection.ConnectionException)] = x.ConnectionException?.Message
            };
        }

        private static DynamicJsonValue GetConnectionStatsDJV(SubscriptionConnectionStats x)
        {
            return new DynamicJsonValue()
            {
                [nameof(SubscriptionConnectionStats.AckRate)] = x.AckRate.CreateMeterData(),
                [nameof(SubscriptionConnectionStats.BytesRate)] = x.BytesRate.CreateMeterData(),
                [nameof(SubscriptionConnectionStats.ConnectedAt)] = x.ConnectedAt,
                [nameof(SubscriptionConnectionStats.DocsRate)] = x.DocsRate.CreateMeterData(),
                [nameof(SubscriptionConnectionStats.LastAckReceivedAt)] = x.LastAckReceivedAt,
                [nameof(SubscriptionConnectionStats.LastMessageSentAt)] = x.LastMessageSentAt,
            };
        }

        [RavenAction("/databases/*/subscriptions/drop", "POST", AuthorizationStatus.ValidUser)]
        public Task DropSubscriptionConnection()
        {
            var subscriptionId = GetLongQueryString("id", required: false);
            var subscriptionName = GetStringQueryString("name", required: false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscription = Database
                    .SubscriptionStorage
                    .GetRunningSubscription(context, subscriptionId, subscriptionName, false);

                if (subscription != null)
                {
                    if (Database.SubscriptionStorage.DropSubscriptionConnection(subscription.SubscriptionId,
                            new SubscriptionClosedException("Dropped by API request")) == false)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }
                }
            }


            return NoContent();
        }
    }

    public class DocumentWithException : IDynamicJson
    {
        public string Id { get; set; }
        public string ChangeVector { get; set; }
        public string Exception { get; set; }
        public object DocumentData { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Exception)] = Exception,
                [nameof(DocumentData)] = DocumentData
            };
        }
    }
}
