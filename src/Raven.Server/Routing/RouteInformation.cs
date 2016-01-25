using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.AspNet.Http;
using Raven.Server.Web;

namespace Raven.Server.Routing
{
    public delegate Task RequestHandler(RequestHandlerContext ctx);

    public class RouteInformation
    {
        public readonly string Path;

        public RequestHandler Get;
        public RequestHandler Put;

        public RouteInformation(string path)
        {
            Path = path;
        }

        public void Build(MemberInfo memberInfo, string method)
        {
            // CurrentRequestContext currentRequestContext
            var currentRequestContext = Expression.Parameter(typeof (RequestHandlerContext), "currentRequestContext");
            // new Handler(currentRequestContext)
            var constructorInfo = memberInfo.DeclaringType.GetConstructors().Single();
            var newExpression = Expression.New(constructorInfo, currentRequestContext);
            // .Handle();
            var handleExpr = Expression.Call(newExpression, memberInfo.Name, new Type[0]);
            var requestDelegate = Expression.Lambda<RequestHandler>(handleExpr, currentRequestContext).Compile();
            
            //TODO: Verify we don't have two methods on the same path & method!
            switch (method)
            {
                case "GET":
                    Get = requestDelegate;
                    break;
                case "PUT":
                    Put = requestDelegate;
                    break;
                default:
                    throw new NotSupportedException("There is no handler for " + method);
            }
        }

        public RequestHandler CreateHandler(HttpContext context)
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