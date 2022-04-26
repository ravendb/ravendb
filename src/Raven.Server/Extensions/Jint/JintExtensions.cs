using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.Linq;
using Esprima.Ast;
using Jint;
using Jint.Constraints;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Native.Function;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Raven.Client.Util;

namespace Raven.Server.Extensions.Jint
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

        public static void ExecuteWithReset(this Engine engine, string source, bool throwExceptionOnError = true)
        {
            try
            {
                engine.Execute(source);
            }
            catch
            {
                if (throwExceptionOnError)
                    throw;
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
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ClrFunctionInstance CreateClrCallBack(this Engine engine, string propertyName, Func<JsValue, JsValue[], JsValue> func)
        {
            return new ClrFunctionInstance(engine, propertyName, func);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SetGlobalClrCallBack(this Engine engine, string propertyName, Func<JsValue, JsValue[], JsValue> func)
        {
            engine.SetGlobalProperty(propertyName, engine.CreateClrCallBack(propertyName, func));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static JsValue CreateEmptyArray(this Engine engine)
        {
            return engine.FromObject(System.Array.Empty<JsValue>());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static JsValue CreateArray(this Engine engine, JsValue[] jsItems)
        {
            return engine.FromObject(jsItems);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SetGlobalProperty(this Engine engine, string propertyName, JsValue value)
        {
            engine.SetValue(propertyName, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static JsValue FromObject(this Engine engine, object value)
        {
            return JsValue.FromObject(engine, value);            
        }

        public static string TryGetFieldFromSimpleLambdaExpression(this IFunction function)
        {
            if (!(function.Params.FirstOrDefault() is Identifier identifier))
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
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static JsValue Call(this ObjectInstance obj, string functionName, JsValue _this, params JsValue[] args)
        {
            var funcProp = obj.GetProperty(functionName);
            if (!(funcProp.Value is FunctionInstance func))
                throw new NotSupportedException($"Not supported for non object value.");
            
            return func.Call(_this, args);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static JsValue StaticCall(this ObjectInstance obj, string functionName, params JsValue[] args)
        {
            return obj.Call(functionName, JsValue.Null, args);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static JsValue Call(this ObjectInstance obj, JsValue _this, params JsValue[] args)
        {
            if (obj == null || !(obj is ICallable func))
                throw new NotSupportedException($"Not supported for non object value.");

            return func.Call(_this, args);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static JsValue StaticCall(this ObjectInstance obj, params JsValue[] args)
        {
            return obj.Call(JsValue.Null, args);
        }


        public static IDisposable ChangeMaxStatements(this Engine engine, int value)
        {
            var maxStatements = engine.FindConstraint<MaxStatements>();
            if (maxStatements == null)
                return null;

            var oldMaxStatements = maxStatements.Max;
            maxStatements.Change(value);

            return new DisposableAction(() => maxStatements.Change(oldMaxStatements));
        }

        public static IDisposable DisableMaxStatements(this Engine engine)
        {
            return ChangeMaxStatements(engine, int.MaxValue);
        }

    }
}
