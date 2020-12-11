// -----------------------------------------------------------------------
//  <copyright file="ResourceApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http.Controllers;
using System.Web.Http.Routing;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Abstractions.Exceptions;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Common
{
    public abstract class ResourceApiController<TResource, TResourceLandlord> : RavenBaseApiController, IResourceApiController<TResource>
        where TResource : IResourceStore
        where TResourceLandlord : IResourceLandlord<TResource>
    {
        public abstract ResourceType ResourceType { get; }

        private int? _maxSecondsForTaskToWaitForResourceToLoad;
        public int MaxSecondsForTaskToWaitForResourceToLoad
        {
            get
            {
                if (_maxSecondsForTaskToWaitForResourceToLoad.HasValue)
                    return _maxSecondsForTaskToWaitForResourceToLoad.Value;

                switch (ResourceType)
                {
                    case ResourceType.Database:
                        _maxSecondsForTaskToWaitForResourceToLoad = _resourceLandlord.GetSystemConfiguration().MaxSecondsForTaskToWaitForDatabaseToLoad;
                        break;
                    case ResourceType.FileSystem:
                    case ResourceType.TimeSeries:
                    case ResourceType.Counter:
                        _maxSecondsForTaskToWaitForResourceToLoad = 30;
                        break;
                    default:
                        throw new NotSupportedException(ResourceType.ToString());
                }

                return _maxSecondsForTaskToWaitForResourceToLoad.Value;
            }
        }

        private string _resourcePrefix;
        public override string ResourcePrefix
        {
            get
            {
                if (string.IsNullOrEmpty(_resourcePrefix))
                {
                    switch (ResourceType)
                    {
                        case ResourceType.Counter:
                            _resourcePrefix = "cs/";
                            break;
                        case ResourceType.TimeSeries:
                            _resourcePrefix = "ts/";
                            break;
                        case ResourceType.Database:
                            _resourcePrefix = string.Empty;
                            break;
                        case ResourceType.FileSystem:
                            _resourcePrefix = "fs/";
                            break;
                        default:
                            throw new NotSupportedException(ResourceType.ToString());
                    }
                }

                return _resourcePrefix;
            }
        }

        public override string ResourceName { get; protected set; }

        private SemaphoreSlim _maxNumberOfThreadsForResourceToLoadSemaphore;

        public SemaphoreSlim MaxNumberOfThreadsForResourceToLoadSemaphore
        {
            get
            {
                return _maxNumberOfThreadsForResourceToLoadSemaphore;
            }
        }

        IResourceStore IResourceApiController.Resource
        {
            get
            {
                return Resource;
            }
        }

        public override InMemoryRavenConfiguration ResourceConfiguration
        {
            get
            {
                return Resource.Configuration;
            }
        }

        private TResourceLandlord _resourceLandlord;

        public TResourceLandlord ResourceLandlord
        {
            get
            {
                if (_resourceLandlord != null)
                {
                    return _resourceLandlord;
                }

                if (Configuration == null)
                {
                    return _resourceLandlord;
                }

                return _resourceLandlord = (TResourceLandlord)Configuration.Properties[typeof(TResourceLandlord)];
            }
        }

        private TResource _resource;

        public TResource Resource
        {
            get
            {
                if (_resource != null)
                {
                    return _resource;
                }

                var resource = ResourceLandlord.GetResourceInternal(ResourceName);
                if (resource == null)
                {
                    throw new InvalidOperationException("Could not find a resource named: " + ResourceName);
                }

                return _resource = resource.Result;
            }
        }

        public DocumentDatabase SystemDatabase
        {
            get
            {
                return DatabasesLandlord.SystemDatabase;
            }
        }

        public override InMemoryRavenConfiguration SystemConfiguration
        {
            get
            {
                return DatabasesLandlord.SystemConfiguration;
            }
        }

        public void SetResource(TResource resource)
        {
            _resource = resource;
        }

        protected bool EnsureSystemDatabase()
        {
            if (Resource == null)
            {
                return false;
            }

            return Resource.Name == null || Resource.Name == Constants.SystemDatabase;
        }

        protected override void InnerInitialization(HttpControllerContext controllerContext)
        {
            base.InnerInitialization(controllerContext);

            _maxNumberOfThreadsForResourceToLoadSemaphore = (SemaphoreSlim)controllerContext.Configuration.Properties[Constants.MaxConcurrentRequestsForDatabaseDuringLoad];
            _resourceLandlord = (TResourceLandlord)controllerContext.Configuration.Properties[typeof(TResourceLandlord)];

            ResourceName = GetResourceName(controllerContext, ResourceType);
        }

        private string GetResourceName(HttpControllerContext controllerContext, ResourceType resourceType)
        {
            var resourceNameUrlKey = GetResourceNameUrlKey(resourceType);
            var values = controllerContext.Request.GetRouteData().Values;
            if (values.ContainsKey("MS_SubRoutes"))
            {
                var routeDatas = (IHttpRouteData[])controllerContext.Request.GetRouteData().Values["MS_SubRoutes"];
                var method = controllerContext.Request.Method;
                return routeDatas
                    .Where(x => x.Values.ContainsKey(resourceNameUrlKey) && x.Route.DataTokens.ContainsKey("actions") && ((HttpActionDescriptor[])x.Route.DataTokens["actions"]).Any(y => y.SupportedHttpMethods.Contains(method)))
                    .Select(x => x.Values[resourceNameUrlKey] as string)
                    .FirstOrDefault();
            }

            if (values.ContainsKey(resourceNameUrlKey))
            {
                return values[resourceNameUrlKey] as string;
            }

            return null;
        }

        public override async Task<RequestWebApiEventArgs> TrySetupRequestToProperResource()
        {
            var resourceName = ResourceName;
            var landlord = ResourceLandlord;

            if (string.IsNullOrWhiteSpace(resourceName) || resourceName == Constants.SystemDatabase)
            {
                DatabasesLandlord.LastRecentlyUsed.AddOrUpdate("System", SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);

                return new RequestWebApiEventArgs { Controller = this, IgnoreRequest = false, TenantId = "System", Resource = SystemDatabase, ResourceType = ResourceType.Database };
            }

            Task<TResource> resourceStoreTask;
            bool hasResource;
            string msg;
            try
            {
                hasResource = landlord.TryGetOrCreateResourceStore(resourceName, out resourceStoreTask);
            }
            catch (ConcurrentLoadTimeoutException e)
            {
                msg = string.Format("The resource '{0}' is currently being loaded, but there are too many requests waiting for resource load. Please try again later, resource loading continues.", resourceName);
                Log.WarnException(msg, e);
                throw new HttpException(503, msg, e);
            }
            catch (Exception e)
            {
                msg = "Could not open resource named: " + resourceName + " " + e.Message;
                Log.WarnException(msg, e);
                throw new HttpException(503, msg, e);
            }
            if (hasResource)
            {
                try
                {
                    int timeToWaitForResourceToLoad = MaxSecondsForTaskToWaitForResourceToLoad;
                    if (resourceStoreTask.IsCompleted == false && resourceStoreTask.IsFaulted == false)
                    {
                        if (MaxNumberOfThreadsForResourceToLoadSemaphore.Wait(0) == false)
                        {
                            msg = string.Format("The resource {0} is currently being loaded, but there are too many requests waiting for resource load. Please try again later, resource loading continues.", resourceName);
                            Log.Warn(msg);
                            throw new TimeoutException(msg);
                        }

                        try
                        {
                            using (var cancellationTokenSource = new CancellationTokenSource())
                            {
                                if (await Task.WhenAny(resourceStoreTask, Task.Delay(TimeSpan.FromSeconds(timeToWaitForResourceToLoad), cancellationTokenSource.Token).IgnoreUnobservedExceptions()).ConfigureAwait(false) != resourceStoreTask)
                                {
                                    cancellationTokenSource.Cancel();
                                    msg = string.Format("The resource {0} is currently being loaded, but after {1} seconds, this request has been aborted. Please try again later, resource loading continues.", resourceName, timeToWaitForResourceToLoad);
                                    Log.Warn(msg);
                                    throw new TimeoutException(msg);
                                }
                            }
                        }
                        finally
                        {
                            MaxNumberOfThreadsForResourceToLoadSemaphore.Release();
                        }
                    }

                    landlord.LastRecentlyUsed.AddOrUpdate(resourceName, SystemTime.UtcNow, (s, time) => SystemTime.UtcNow);

                    return new RequestWebApiEventArgs { Controller = this, IgnoreRequest = false, TenantId = resourceName, Resource = resourceStoreTask.Result, ResourceType = ResourceType };
                }
                catch (Exception e)
                {
                    msg = "Could not open resource named: " + resourceName + Environment.NewLine + e;

                    Log.WarnException(msg, e);
                    throw new HttpException(503, msg, e);
                }
            }

            msg = "Could not find a resource named: " + resourceName;
            Log.Warn(msg);
            throw new HttpException(503, msg);
        }

        public bool RejectClientRequests
        {
            get
            {
                switch (ResourceType)
                {
                    case ResourceType.Database:
                        return Resource.Configuration.RejectClientsMode;
                    case ResourceType.FileSystem:
                    case ResourceType.Counter:
                    case ResourceType.TimeSeries:
                        return false;
                    default:
                        throw new NotSupportedException(ResourceType.ToString());
                }
            }
        }

        public override async Task<HttpResponseMessage> ExecuteAsync(HttpControllerContext controllerContext, CancellationToken cancellationToken)
        {
            InnerInitialization(controllerContext);

            HttpResponseMessage msg;
            if (IsClientV4OrHigher(out msg))
                return msg;

            var authorizer = (MixedModeRequestAuthorizer)controllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];
            var result = new HttpResponseMessage();
            if (InnerRequest.Method.Method != "OPTIONS")
            {
                result = await RequestManager.HandleActualRequest(this, controllerContext, async () =>
                {
                    RequestManager.SetThreadLocalState(ReadInnerHeaders, ResourceName);
                    return await ExecuteActualRequest(controllerContext, cancellationToken, authorizer).ConfigureAwait(false);
                }, httpException =>
                {
                    var response = GetMessageWithObject(new { Error = httpException.Message }, HttpStatusCode.ServiceUnavailable);

                    var timeout = httpException.InnerException as TimeoutException;
                    if (timeout != null)
                    {
                        response.Headers.Add("Raven-Database-Load-In-Progress", ResourceName);
                    }
                    return response;
                }).ConfigureAwait(false);
            }

            RequestManager.AddAccessControlHeaders(this, result);
            RequestManager.ResetThreadLocalState();

            return result;
        }

        private async Task<HttpResponseMessage> ExecuteActualRequest(HttpControllerContext controllerContext, CancellationToken cancellationToken, MixedModeRequestAuthorizer authorizer)
        {
            if (SkipAuthorizationSinceThisIsMultiGetRequestAlreadyAuthorized == false)
            {
                HttpResponseMessage authMsg;
                if (authorizer.TryAuthorize(this, out authMsg) == false)
                {
                    return authMsg;
                }
            }

            if (IsInternalRequest == false)
            {
                RequestManager.IncrementRequestCount();
            }

            if (ResourceName != null && await ResourceLandlord.GetResourceInternal(ResourceName).ConfigureAwait(false) == null)
            {
                var msg = "Could not find a resource named: " + ResourceName;
                return GetMessageWithObject(new { Error = msg }, HttpStatusCode.ServiceUnavailable);
            }

            var sp = Stopwatch.StartNew();
            controllerContext.Request.Properties["timer"] = sp;
            controllerContext.RequestContext.Principal = CurrentOperationContext.User.Value;
            var result = await base.ExecuteAsync(controllerContext, cancellationToken).ConfigureAwait(false);
            sp.Stop();
            AddRavenHeader(result, sp);

            return result;
        }

        protected string GetResourceNameUrlKey(ResourceType resourceType)
        {
            switch (resourceType)
            {
                case ResourceType.Counter:
                    return "counterStorageName";
                case ResourceType.Database:
                    return "databaseName";
                case ResourceType.FileSystem:
                    return "fileSystemName";
                case ResourceType.TimeSeries:
                    return "timeSeriesName";
                default:
                    throw new NotSupportedException(resourceType.ToString());
            }
        }
    }
}
