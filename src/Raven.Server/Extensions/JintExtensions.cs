using System.Linq;
using Esprima.Ast;

namespace Raven.Server.Extensions
{
    public static class JintExtensions
    {
        public static string TryGetFieldFromSimpleLambdaExpression(this IFunction function)
        {
            if (!(function.Params.FirstOrDefault() is Identifier identifier))
                return null;
            if (!(function.Body.Body.FirstOrDefault() is ReturnStatement returnStatement))
                return null;
            if (!(returnStatement.Argument is MemberExpression me))
                return null;
            if (!(me.Property is Identifier property))
                return null;
            if ((!(me.Object is Identifier reference) || reference.Name != identifier.Name))
                return null;
            return property.Name;
        }
    }
}
