using System;
using System.Collections.Generic;
using System.Linq;
using Esprima.Ast;
using Jint;
using Jint.Constraints;
using Jint.Native;
using Jint.Native.Array;
using Jint.Runtime.Descriptors;
using Raven.Client.Util;

namespace Raven.Server.Extensions
{
    public static class JintExtensions
    {
        public static IEnumerable<KeyValuePair<JsValue, PropertyDescriptor>> GetOwnPropertiesWithoutLength(this ArrayInstance array)
        {
            foreach (var kvp in array.GetOwnProperties())
            {
                if (kvp.Key == "length")
                    continue;

                yield return kvp;
            }
        }

        public static IDisposable ChangeMaxStatements(this Engine engine, int value)
        {
            var constraint = engine.FindConstraint<MaxStatementsConstraint>();
            if (constraint == null)
                return null;

            var oldMaxStatements = constraint.MaxStatements;
            constraint.MaxStatements = value;

            return new DisposableAction(() => constraint.MaxStatements = oldMaxStatements);
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
                engine.ResetCallStack();
                engine.ResetConstraints();
            }
        }

        public static void ExecuteWithReset(this Engine engine, Script script)
        {
            try
            {
                engine.Execute(script);
            }
            finally
            {
                engine.ResetCallStack();
                engine.ResetConstraints();
            }
        }

        public static string TryGetFieldFromSimpleLambdaExpression(this IFunction function)
        {
            if (function.Params.Count == 0 || function.Params[0] is not Identifier identifier)
                return null;

            var me = GetMemberExpression(function);
            if (me == null)
                return null;

            if (!(me.Property is Identifier property))
                return null;
            if ((!(me.Object is Identifier reference) || reference.Name != identifier.Name))
                return null;
            return property.Name;
        }

        private static MemberExpression GetMemberExpression(IFunction function)
        {
            switch (function)
            {
                case ArrowFunctionExpression afe:
                    return afe.ChildNodes.LastOrDefault() as StaticMemberExpression;
                default:
                    if (!(function.Body.ChildNodes.FirstOrDefault() is ReturnStatement rs))
                        return null;
                    return rs.Argument as MemberExpression;
            }
        }
    }
}
