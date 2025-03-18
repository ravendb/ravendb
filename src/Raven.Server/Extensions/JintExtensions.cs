using System;
using System.Collections.Generic;
using System.Linq;
using Acornima.Ast;
using Jint;
using Jint.Constraints;
using Jint.Native;
using Raven.Client.Util;

namespace Raven.Server.Extensions
{
    public static class JintExtensions
    {
        public static IEnumerable<KeyValuePair<string, JsValue>> GetOwnPropertiesWithoutLength(this JsArray array)
        {
            return array.GetEntries();
        }

        public static IDisposable ChangeMaxStatements(this Engine engine, int value)
        {
            var maxStatements = engine.Constraints.Find<MaxStatementsConstraint>();
            if (maxStatements == null)
                return null;

            var oldMaxStatements = maxStatements.MaxStatements;
            maxStatements.MaxStatements = value;

            return new DisposableAction(() => maxStatements.MaxStatements = oldMaxStatements);
        }

        public static IDisposable DisableMaxStatements(this Engine engine)
        {
            return ChangeMaxStatements(engine, int.MaxValue);
        }

        public static void ExecuteWithReset(this Engine engine, string source)
        {
            try
            {
                engine.Execute(source);
            }
            finally
            {
                engine.Advanced.ResetCallStack();
                engine.Constraints.Reset();
            }
        }

        public static string TryGetFieldFromSimpleLambdaExpression(this IFunction function)
        {
            if (function.Params.Count == 0 || function.Params[0] is not Identifier identifier)
                return null;

            var me = GetMemberExpression(function);

            if (me?.Property is not Identifier property)
                return null;

            if (me.Object is not Identifier reference || reference.Name != identifier.Name)
                return null;

            return property.Name;
        }

        private static MemberExpression GetMemberExpression(IFunction function)
        {
            switch (function)
            {
                case ArrowFunctionExpression afe:
                    return afe.ChildNodes.LastOrDefault() as MemberExpression;
                default:
                    if (!(function.Body.ChildNodes.FirstOrDefault() is ReturnStatement rs))
                        return null;
                    return rs.Argument as MemberExpression;
            }
        }
    }
}
