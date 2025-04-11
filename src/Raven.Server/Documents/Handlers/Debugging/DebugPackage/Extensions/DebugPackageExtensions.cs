using System;
using System.Linq;
using System.Linq.Expressions;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Extensions;

public class DebugPackageExtensions
{
    public static string GetPackageEntryName<T>(Expression<Func<T, object>> debugEndpoint, string prefix = null, string extension = "json")
    {
        var methodBody = debugEndpoint.Body;
    
        if (methodBody is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            methodBody = unary.Operand;
    
        if (methodBody is MethodCallExpression methodCall)
        {
            var method = methodCall.Method;
            var ravenActionAttribute = method
                .GetCustomAttributes(typeof(RavenActionAttribute), false)
                .FirstOrDefault() as RavenActionAttribute;

            if (ravenActionAttribute == null)
                throw new InvalidOperationException($"Method '{method.Name}' does not contain '{nameof(RavenActionAttribute)}'. Please make sure that this is a valid handler method.");

            if (ravenActionAttribute.IsDebugInformationEndpoint == false)
                throw new InvalidOperationException($"Endpoint '{ravenActionAttribute.Path}' is not a debug information endpoint.");
            
            return DebugInfoPackageUtils.GetOutputPathFromRouteInformation(ravenActionAttribute.Path, prefix, extension) ;
        }
    
        throw new InvalidOperationException("The expression does not represent a method call.");
    }
}
