using System.Web;
using System.Web.Routing;

namespace Raven.Imports.SignalR.Hosting.AspNet.Routing
{
    public class IncomingOnlyRouteConstraint : IRouteConstraint
    {
        public bool Match(HttpContextBase httpContext, Route route, string parameterName, RouteValueDictionary values, RouteDirection routeDirection)
        {
            if (routeDirection == RouteDirection.IncomingRequest)
            {
                return true;
            }
            return false;
        }
    }
}
