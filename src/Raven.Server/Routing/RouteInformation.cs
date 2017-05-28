using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Database;
using Raven.Server.Documents;
using Raven.Server.Web;
using Sparrow;

namespace Raven.Server.Routing
{
    public delegate Task HandleRequest(RequestHandlerContext ctx);

    public class RouteInformation
    {
        public readonly string Method;
        public readonly string Path;

        public readonly bool NoAuthorizationRequired;
        public readonly bool SkipUsagesCount;

        private HandleRequest _request;
        private RouteType _typeOfRoute;

        private enum RouteType
        {
            None,
            Databases
        }

        public RouteInformation(string method, string path, bool noAuthorizationRequired, bool skipUsagesCount)
        {
            Method = method;
            Path = path;
            NoAuthorizationRequired = noAuthorizationRequired;
            SkipUsagesCount = skipUsagesCount;
        }

        public void Build(MethodInfo action)
        {
            if (action.ReturnType != typeof(Task))
                throw new InvalidOperationException(action.DeclaringType.FullName + "." + action.Name +
                                                    " must return Task");

            if (typeof(DatabaseRequestHandler).IsAssignableFrom(action.DeclaringType))
            {
                _typeOfRoute = RouteType.Databases;
            }

            // CurrentRequestContext currentRequestContext
            var currentRequestContext = Expression.Parameter(typeof(RequestHandlerContext), "currentRequestContext");
            // new Handler(currentRequestContext)
            var constructorInfo = action.DeclaringType.GetConstructor(new Type[0]);
            var newExpression = Expression.New(constructorInfo);
            var handler = Expression.Parameter(action.DeclaringType, "handler");

            var block = Expression.Block(typeof(Task), new[] { handler },
                Expression.Assign(handler, newExpression),
                Expression.Call(handler, "Init", new Type[0], currentRequestContext),
                Expression.Call(handler, action.Name, new Type[0]));
            // .Handle();
            _request = Expression.Lambda<HandleRequest>(block, currentRequestContext).Compile();
        }

        public Task CreateDatabase(RequestHandlerContext context)
        {
            var databaseName = context.RouteMatch.GetCapture();
            var databasesLandlord = context.RavenServer.ServerStore.DatabasesLandlord;
            var database = databasesLandlord.TryGetOrCreateResourceStore(databaseName);

            if (database == null)
            {
                ThrowDatabaseDoesNotExist(databaseName);
                return Task.CompletedTask;// never hit
            }

            if (database.IsCompleted)
            {
                context.Database = database.Result;
                return context.Database.DatabaseShutdown.IsCancellationRequested == false
                    ? Task.CompletedTask
                    : UnlikelyWaitForDatabaseToUnload(context, context.Database, databasesLandlord, databaseName);
            }

            return UnlikelyWaitForDatabaseToLoad(context, database, databasesLandlord, databaseName);
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
            var result = await Task.WhenAny(database, Task.Delay(time));
            if (result != database)
            {
                ThrowDatabaseLoadTimeout(databaseName, databasesLandlord.DatabaseLoadTimeout);
            }
            context.Database = await database;
        }

        private static void ThrowDatabaseUnloadTimeout(StringSegment databaseName, TimeSpan timeout)
        {
            throw new DatabaseLoadTimeoutException($"Timeout when unloading database {databaseName} after {timeout}, try again later");
        }

        private static void ThrowDatabaseLoadTimeout(StringSegment databaseName, TimeSpan timeout)
        {
            throw new DatabaseLoadTimeoutException($"Timeout when loading database {databaseName} after {timeout}, try again later");
        }

        private static void ThrowDatabaseDoesNotExist(StringSegment databaseName)
        {
            throw new DatabaseDoesNotExistException($"Database '{databaseName}' was not found");
        }

        public Tuple<HandleRequest,Task<HandleRequest>> TryGetHandler(RequestHandlerContext context)
        {
            if (_typeOfRoute == RouteType.None)
            {
                return Tuple.Create<HandleRequest, Task<HandleRequest>>(_request, null);
            }
            var database = CreateDatabase(context);
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
            return $"{nameof(Method)}: {Method}, {nameof(Path)}: {Path}, {nameof(NoAuthorizationRequired)}: {NoAuthorizationRequired}";
        }
    }
}