using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Json;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Conventions;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;

namespace Raven.Server.Documents.Handlers
{
    public class SubscriptionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/subscriptions/try", "POST", AuthorizationStatus.ValidUser)]
        public async Task Try()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var tryout = JsonDeserializationServer.SubscriptionTryout(json);

                SubscriptionPatchDocument patch = null;
                if (string.IsNullOrEmpty(tryout.Script) == false)
                {
                    patch = new SubscriptionPatchDocument(Database, tryout.Script);
                }

                if (tryout.Collection == null)
                    throw new ArgumentException("Collection must be specified");

                var pageSize = GetIntValueQueryString("pageSize") ?? 1;

                var fetcher = new SubscriptionDocumentsFetcher(Database, pageSize, -0x42,
                    new IPEndPoint(HttpContext.Connection.RemoteIpAddress, HttpContext.Connection.RemotePort));

                var state = new SubscriptionState
                {
                    ChangeVectorForNextBatchStartingPoint = tryout.ChangeVector,
                    Criteria = new SubscriptionCriteria
                    {
                        Collection = tryout.Collection,
                        IncludeRevisions = tryout.IncludeRevisions,
                        Script = tryout.Script
                    }
                };

                if (Enum.TryParse(
                    tryout.ChangeVector,
                    out Constants.Documents.SubscriptionChangeVectorSpecialStates changeVectorSpecialValue))
                {
                    switch (changeVectorSpecialValue)
                    {
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.BeginningOfTime:
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange:
                            state.ChangeVectorForNextBatchStartingPoint= null;
                            break;
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument:
                            state.ChangeVectorForNextBatchStartingPoint = Database.DocumentsStorage.GetLastDocumentChangeVector(context, state.Criteria.Collection);
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

                        foreach (var itemDetails in fetcher.GetDataToSend(context, state, patch, 0))
                        {
                            if(itemDetails.Doc.Data == null)
                                continue;
                            
                            if (first == false)
                                writer.WriteComma();

                            if (itemDetails.Exception == null)
                            {
                                writer.WriteDocument(context, itemDetails.Doc);
                            }
                            else
                            {
                                var docWithExcepton = new DocumentWithException
                                {
                                    Exception = itemDetails.Exception.ToString(),
                                    ChangeVector = itemDetails.Doc.ChangeVector,
                                    Id = itemDetails.Doc.Id,
                                    DocumentData = itemDetails.Doc.Data
                                };
                                writer.WriteObject(context.ReadObject(docWithExcepton.ToJson(), ""));
                            }

                            first = false;
                        }
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }
            }
        }
        
        [RavenAction("/databases/*/subscriptions", "PUT", AuthorizationStatus.ValidUser)]
        public async Task Create()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using(context.OpenReadTransaction())
            {

                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var options = JsonDeserializationServer.SubscriptionCreationParams(json);
                if (Constants.Documents.SubscriptionChangeVectorSpecialStates.TryParse(
                    options.ChangeVector,
                    out Constants.Documents.SubscriptionChangeVectorSpecialStates changeVectorSpecialValue))
                {
                    switch (changeVectorSpecialValue)
                    {
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.BeginningOfTime:
                            options.ChangeVector = null;
                            break;
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument:
                            options.ChangeVector = Database.DocumentsStorage.GetLastDocumentChangeVector(context, options.Criteria.Collection);
                            break;
                    }
                }
                var id = GetLongQueryString("id", required: false);
                var disabled = GetBoolValueQueryString("disabled", required: false);
                var subscriptionId = await Database.SubscriptionStorage.PutSubscription(options, id, disabled);
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

                    var subscriptionsAsBlittable = subscriptions.Select(x => EntityToBlittable.ConvertEntityToBlittable(new
                    {
                        x.SubscriptionId,
                        x.SubscriptionName,
                        LatestChangeVectorClientACKnowledged = x.ChangeVectorForNextBatchStartingPoint,
                        x.Criteria,
                        x.Connection?.SubscriptionState.LastTimeServerMadeProgressWithDocuments
                        ,
                        Connection = new
                        {
                            x.Connection?.ClientUri,
                            x.Connection?.Strategy,
                            x.Connection?.Stats,
                            ConnectionException = x.Connection?.ConnectionException?.Message
                        },
                        RecentConnections = x.RecentConnections?.Select(r=> new
                        {
                            r.SubscriptionState.SubscriptionId,
                            r.SubscriptionState.SubscriptionName,
                            State = new
                            {
                                LatestChangeVectorClientACKnowledged = r.SubscriptionState.ChangeVectorForNextBatchStartingPoint,
                                r.SubscriptionState.Criteria
                            },
                            Connection = new
                            {
                                r.ClientUri,
                                r.Options.Strategy,
                                r.Stats,
                                r.ConnectionException?.Message
                            }
                        }).ToList(),
                        FailedConnections = x.RecentRejectedConnections?.Select(r => new
                        {
                            r.SubscriptionState.SubscriptionId,
                            r.SubscriptionState.SubscriptionName,
                            State = new
                            {
                                LatestChangeVectorClientACKnowledged = r.SubscriptionState.ChangeVectorForNextBatchStartingPoint,
                                r.SubscriptionState.Criteria
                            },
                            Connection = new
                            {
                                r.ClientUri,
                                r.Options.Strategy,
                                r.Stats,
                                r.ConnectionException?.Message
                            }
                        }).ToList()
                        
                    }, DocumentConventions.Default, context));
                    writer.WriteArray(context, "Results", subscriptionsAsBlittable, (w, c, subscription) =>
                    {
                        c.Write(w, subscription);
                    });

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/subscriptions/drop", "POST", AuthorizationStatus.ValidUser)]
        public Task DropSubscriptionConnection()
        {
            var subscriptionId = GetLongQueryString("id");
            
            if (Database.SubscriptionStorage.DropSubscriptionConnection(subscriptionId, new SubscriptionClosedException("Dropped by API request")) == false)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
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
