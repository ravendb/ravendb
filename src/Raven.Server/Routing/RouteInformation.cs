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
        public readonly string Path;

        private HandleRequest _get;
        private HandleRequest _put;
        private HandleRequest _post;
        private HandleRequest _delete;

        private enum RouteType
        {
            None,
            Databases
        }

        private RouteType _typeOfRoute;

        public RouteInformation(string path)
        {
            Path = path;
        }

        public void Build(MemberInfo memberInfo, string method)
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
            var requestDelegate = Expression.Lambda<HandleRequest>(block, currentRequestContext).Compile();
            
            //TODO: Verify we don't have two methods on the same path & method!
            switch (method)
            {
                case "GET":
                    _get = requestDelegate;
                    break;
                case "PUT":
                    _put = requestDelegate;
                    break;
                case "POST":
                    _post = requestDelegate;
                    break;
                case "DELETE":
                    _delete = requestDelegate;
                    break;
                default:
                    throw new NotSupportedException("There is no handler for " + method);
            }
        }

        public async Task CreateDatabase(RequestHandlerContext context)
        {
            var databaseId = context.RouteMatch.GetCapture();
            var databasesLandlord = context.ServerStore.DatabasesLandlord;
            Task<DocumentsStorage> task;
            if (databasesLandlord.TryGetOrCreateResourceStore(databaseId, out task) == false)
            {
                throw new DatabaseDoesNotExistsException($"Database '{databaseId}' was not found");
            }
            context.DocumentsStorage = await task;
        }

        public async Task<HandleRequest> CreateHandler(RequestHandlerContext context)
        {
            switch (_typeOfRoute)
            {
                case RouteType.Databases:
                    await CreateDatabase(context);
                    break;
            }

            switch (context.HttpContext.Request.Method)
            {
                case "GET":
                    return _get;
                case "PUT":
                    return _put;
                case "DELETE":
                    return _delete;
                case "POST":
                    return _post;
                default:
                {
                    throw new NotSupportedException("There is no handler for " + context.HttpContext.Request.Method);
                }
            }
        }
    }
}