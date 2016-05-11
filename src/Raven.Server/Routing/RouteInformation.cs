using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

using Raven.Server.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Web;

namespace Raven.Server.Routing
{
    public delegate Task HandleRequest(RequestHandlerContext ctx);

    public class RouteInformation
    {
        public readonly string Method;
        public readonly string Path;

        public readonly bool NoAuthorizationRequired;

        private HandleRequest _request;
        private RouteType _typeOfRoute;

        private enum RouteType
        {
            None,
            Databases
        }

        public RouteInformation(string method, string path, bool noAuthorizationRequired)
        {
            Method = method;
            Path = path;
            NoAuthorizationRequired = noAuthorizationRequired;
        }

        public void Build(MethodInfo action)
        {
            if (action.ReturnType != typeof (Task))
                throw new InvalidOperationException(action.DeclaringType.FullName + "." + action.Name +
                                                    " must return Task");

            if (typeof(DatabaseRequestHandler).IsAssignableFrom(action.DeclaringType))
            {
                _typeOfRoute = RouteType.Databases;
            }

            // CurrentRequestContext currentRequestContext
            var currentRequestContext = Expression.Parameter(typeof (RequestHandlerContext), "currentRequestContext");
            // new Handler(currentRequestContext)
            var constructorInfo = action.DeclaringType.GetConstructor(new Type[0]);
            var newExpression = Expression.New(constructorInfo);
            var handler = Expression.Parameter(action.DeclaringType, "handler");

            var block = Expression.Block(typeof(Task),new [] {handler},
                Expression.Assign(handler, newExpression),
                Expression.Call(handler, "Init", new Type[0], currentRequestContext),
                Expression.Call(handler, action.Name, new Type[0]));
            // .Handle();
            _request = Expression.Lambda<HandleRequest>(block, currentRequestContext).Compile();
        }

        public async Task CreateDatabase(RequestHandlerContext context)
        {
            var databaseName = context.RouteMatch.GetCapture();
            var databasesLandlord = context.RavenServer.ServerStore.DatabasesLandlord;
            var database =  databasesLandlord.TryGetOrCreateResourceStore(databaseName);
            if (database == null)
            {
                throw new DatabaseDoesNotExistsException($"Database '{databaseName}' was not found");
            }
            context.Database = await database;
        }

        public async Task<HandleRequest> CreateHandler(RequestHandlerContext context)
        {
            switch (_typeOfRoute)
            {
                case RouteType.Databases:
                    await CreateDatabase(context);
                    break;
            }

            return _request;
        }

        public HandleRequest GetRequestHandler()
        {
            return _request;
        }
    }
}