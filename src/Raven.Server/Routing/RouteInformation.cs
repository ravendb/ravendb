using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Raven.Server.Web;
using Sparrow;

namespace Raven.Server.Routing
{
    public delegate Task HandleRequest(RequestHandlerContext ctx);

    public class RouteInformation
    {
        public AuthorizationStatus AuthorizationStatus;
        public readonly string Method;
        public readonly string Path;

        public readonly bool SkipUsagesCount;
        public readonly CorsMode CorsMode;

        public bool DisableOnCpuCreditsExhaustion;

        private HandleRequest _request;
        private HandleRequest _shardedRequest;
        private RouteType _typeOfRoute;

        public bool IsDebugInformationEndpoint;

        public enum RouteType
        {
            None,
            Databases
        }

        public RouteInformation(string method, string path, AuthorizationStatus authorizationStatus, bool skipUsagesCount, CorsMode corsMode,
            bool isDebugInformationEndpoint = false, 
            bool disableOnCpuCreditsExhaustion = false)
        {
            DisableOnCpuCreditsExhaustion = disableOnCpuCreditsExhaustion;
            AuthorizationStatus = authorizationStatus;
            IsDebugInformationEndpoint = isDebugInformationEndpoint;
            Method = method;
            Path = path;
            SkipUsagesCount = skipUsagesCount;
            CorsMode = corsMode;
        }

        public RouteType TypeOfRoute => _typeOfRoute;

        public void BuildSharded(MethodInfo shardedAction)
        {
            _shardedRequest = BuildInternal(shardedAction);
        }

        public void Build(MethodInfo action)
        {
            if (typeof(DatabaseRequestHandler).IsAssignableFrom(action.DeclaringType))
            {
                _typeOfRoute = RouteType.Databases;
            }

            _request = BuildInternal(action);
        }

        private static HandleRequest BuildInternal(MethodInfo action)
        {
            if (action.ReturnType != typeof(Task))
                throw new InvalidOperationException(action.DeclaringType?.FullName + "." + action.Name +
                                                    " must return Task");
            // CurrentRequestContext currentRequestContext
            var currentRequestContext = Expression.Parameter(typeof(RequestHandlerContext), "currentRequestContext");
            // new Handler(currentRequestContext)
            var constructorInfo = action.DeclaringType.GetConstructor(new Type[0]);
            var newExpression = Expression.New(constructorInfo);
            var handler = Expression.Parameter(action.DeclaringType, "handler");

            var block = Expression.Block(typeof(Task), new[] {handler},
                Expression.Assign(handler, newExpression),
                Expression.Call(handler, nameof(RequestHandler.Init), new Type[0], currentRequestContext),
                Expression.Call(handler, action.Name, new Type[0]));
            // .Handle();
            var r = Expression.Lambda<HandleRequest>(block, currentRequestContext).Compile();
            return r;
        }

        public Task CreateDatabase(RequestHandlerContext context)
        {
            var databaseName = context.RouteMatch.GetCapture();
            if (context.RavenServer.ServerStore.IsPassive())
            {
                throw new NodeIsPassiveException($"Can't perform actions on the database '{databaseName}' while the node is passive.");
            }

            var databasesLandlord = context.RavenServer.ServerStore.DatabasesLandlord;
            var result = databasesLandlord.TryGetOrCreateDatabase(databaseName);
            switch (result.DatabaseStatus)
            {
                case DatabasesLandlord.DatabaseSearchResult.Status.Sharded:
                    return null;

                default:
                    var database = result.DatabaseTask;
                    if (database.IsCompletedSuccessfully)
                    {
                        context.Database = database.Result;

                        if (context.Database == null)
                            DatabaseDoesNotExistException.Throw(databaseName.Value);

                        return context.Database?.DatabaseShutdown.IsCancellationRequested == false
                            ? Task.CompletedTask
                            : UnlikelyWaitForDatabaseToUnload(context, context.Database, databasesLandlord, databaseName);
                    }

                    return UnlikelyWaitForDatabaseToLoad(context, database, databasesLandlord, databaseName);
            }
        }

        private async Task UnlikelyWaitForDatabaseToUnload(RequestHandlerContext context, DocumentDatabase database,
            DatabasesLandlord databasesLandlord, StringSegment databaseName)
        {
            var time = databasesLandlord.DatabaseLoadTimeout;
            if (await database.DatabaseShutdownCompleted.WaitAsync(time) == false)
            {
                ThrowDatabaseUnloadTimeout(databaseName, databasesLandlord.DatabaseLoadTimeout);
            }
            await CreateDatabase(context);
        }

        private static async Task UnlikelyWaitForDatabaseToLoad(RequestHandlerContext context, Task<DocumentDatabase> database,
            DatabasesLandlord databasesLandlord, StringSegment databaseName)
        {
            var time = databasesLandlord.DatabaseLoadTimeout;
            await Task.WhenAny(database, Task.Delay(time));
            if (database.IsCompleted == false)
            {
                if (databasesLandlord.InitLog.TryGetValue(databaseName.Value, out var initLogQueue))
                {
                    var sb = new StringBuilder();
                    foreach (var logline in initLogQueue)
                        sb.AppendLine(logline);

                    ThrowDatabaseLoadTimeoutWithLog(databaseName, databasesLandlord.DatabaseLoadTimeout, sb.ToString());
                }
                ThrowDatabaseLoadTimeout(databaseName, databasesLandlord.DatabaseLoadTimeout);
            }
            context.Database = await database;
            if (context.Database == null)
                DatabaseDoesNotExistException.Throw(databaseName.Value);
        }

        private static void ThrowDatabaseUnloadTimeout(StringSegment databaseName, TimeSpan timeout)
        {
            throw new DatabaseLoadTimeoutException($"Timeout when unloading database {databaseName} after {timeout}, try again later");
        }

        private static void ThrowDatabaseLoadTimeout(StringSegment databaseName, TimeSpan timeout)
        {
            throw new DatabaseLoadTimeoutException($"Timeout when loading database {databaseName} after {timeout}, try again later");
        }

        private static void ThrowDatabaseLoadTimeoutWithLog(StringSegment databaseName, TimeSpan timeout, string log)
        {
            throw new DatabaseLoadTimeoutException($"Database {databaseName} after {timeout} is still loading, try again later. Database initialization log: " + Environment.NewLine + log);
        }

        public Tuple<HandleRequest, Task<HandleRequest>> TryGetHandler(RequestHandlerContext context)
        {
            if (_typeOfRoute == RouteType.None)
            {
                return Tuple.Create<HandleRequest, Task<HandleRequest>>(_request, null);
            }

            var database = CreateDatabase(context);
            if (database == null)
            {
#if DEBUG
                if (_shardedRequest == null)
                {
                    throw new InvalidOperationException("Unable to run request " + context.HttpContext.Request.GetFullUrl() +
                                                        ", the database is sharded, but no shared route is defined for this operation!");
                }
#endif
                return Tuple.Create<HandleRequest, Task<HandleRequest>>(_shardedRequest, null);
            }

            if (database.Status == TaskStatus.RanToCompletion)
            {
                return Tuple.Create<HandleRequest, Task<HandleRequest>>(_request, null);
            }
            return Tuple.Create<HandleRequest, Task<HandleRequest>>(null, WaitForDb(database));
        }

        private async Task<HandleRequest> WaitForDb(Task databaseLoading)
        {
            await databaseLoading;

            return _request;
        }

        public HandleRequest GetRequestHandler()
        {
            return _request;
        }

        public override string ToString()
        {
            return $"{nameof(Method)}: {Method}, {nameof(Path)}: {Path}, {nameof(AuthorizationStatus)}: {AuthorizationStatus}";
        }

    }
}
