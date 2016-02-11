using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
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

        private HandleRequest _request;
        private RouteType _typeOfRoute;

        private enum RouteType
        {
            None,
            Databases
        }

        public RouteInformation(string method, string path)
        {
            Method = method;
            Path = path;
        }

        public void Build(MemberInfo memberInfo)
        {
            if (typeof(DatabaseRequestHandler).IsAssignableFrom(memberInfo.DeclaringType))
            {
                _typeOfRoute = RouteType.Databases;
            }

            // CurrentRequestContext currentRequestContext
            var currentRequestContext = Expression.Parameter(typeof (RequestHandlerContext), "currentRequestContext");
            // new Handler(currentRequestContext)
            var constructorInfo = memberInfo.DeclaringType.GetConstructor(new Type[0]);
            var newExpression = Expression.New(constructorInfo);
            var handler = Expression.Parameter(memberInfo.DeclaringType, "handler");

            var block = Expression.Block(typeof(Task),new [] {handler},
                Expression.Assign(handler, newExpression),
                Expression.Call(handler, "Init", new Type[0], currentRequestContext),
                Expression.Call(handler, memberInfo.Name, new Type[0]));
            // .Handle();
            _request = Expression.Lambda<HandleRequest>(block, currentRequestContext).Compile();
        }

        public async Task CreateDatabase(RequestHandlerContext context)
        {
            var databaseId = context.RouteMatch.GetCapture();
            var databasesLandlord = context.ServerStore.DatabasesLandlord;
            Task<DocumentDatabase> task;
            if (databasesLandlord.TryGetOrCreateResourceStore(databaseId, out task) == false)
            {
                throw new DatabaseDoesNotExistsException($"Database '{databaseId}' was not found");
            }
            context.Database = await task;
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
    }
}