using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.AspNet.Builder;
using Microsoft.AspNet.Http;
using Raven.Server.ServerWide;

namespace Raven.Server.Routing
{
    public class RouteInformation
    {
        public readonly string Path;
        private readonly ServerStore _serverStore;

        public RequestDelegate Get;
        public RequestDelegate Put;

        public RouteInformation(string path, ServerStore serverStore)
        {
            Path = path;
            _serverStore = serverStore;
        }

        public void Build(MemberInfo memberInfo, string method)
        {
            // HttpContext ctx
            var ctx = Expression.Parameter(typeof (HttpContext), "ctx");
            // ServerStore serverStore
            var serverStore = Expression.Parameter(typeof (ServerStore), "serverStore");
            // new Handler(serverStore)
            var constructorInfo = memberInfo.DeclaringType.GetConstructors().Single();
            var newExpression = Expression.New(constructorInfo, serverStore);
            // .Handle(ctx);
            var handleExpr = Expression.Call(newExpression, memberInfo.Name, new Type[0], ctx);
            var requestDelegate = Expression.Lambda<Func<HttpContext, ServerStore, Task>>(handleExpr, ctx, serverStore).Compile();

            RequestDelegate handler = context => requestDelegate(context, _serverStore);
            
            //TODO: Verify we don't have two methods on the same path & method!
            switch (method)
            {
                case "GET":
                    Get = handler;
                    break;
                case "PUT":
                    Put = handler;
                    break;
                default:
                    throw new NotSupportedException("There is no handler for " + method);
            }
        }

        public RequestDelegate CreateHandler(HttpContext context)
        {
            switch (context.Request.Method)
            {
                case "GET":
                    return Get;
                case "PUT":
                    return Put;
                default:
                    throw new NotSupportedException("There is no handler for " + context.Request.Method);
            }
        }
    }
}