using System;
using System.Collections.Generic;
using System.Linq;
using Esprima.Ast;
using Raven.Client.Util;

using V8.Net;

namespace Raven.Server.Extensions
{
    public static class JsV8Extensions
    {
        public static bool IsNumberEx(this InternalHandle value) 
        {
            return value.IsNumber || value.IsNumberObject;
        }

        public static bool IsNumberOrIntEx(this InternalHandle value) 
        {
            return value.IsNumberEx() || value.IsInt32;
        }

        public static bool IsStringEx(this InternalHandle value) 
        {
            return value.IsString || value.IsStringObject;
        }

        public static InternalHandle GetOwnProperty(this V8NativeObject obj, string name)
        {
            return obj._.GetOwnProperty(name);
        }

        public static InternalHandle GetOwnProperty(this InternalHandle obj, string name)
        {
            return obj.GetProperty(name);
        }

        public static InternalHandle GetOwnProperty(this V8NativeObject obj, Int32 index)
        {
            return obj._.GetOwnProperty(index);
        }

        public static InternalHandle GetOwnProperty(this InternalHandle obj, Int32 index)
        {
            return obj.GetProperty(index);
        }

        public static IEnumerable<KeyValuePair<string, InternalHandle>> GetProperties(this V8NativeObject obj)
        {
            return obj._.GetProperties();
        }

        public static IEnumerable<KeyValuePair<string, InternalHandle>> GetOwnProperties(this V8NativeObject obj)
        {
            return obj._.GetOwnProperties();
        }

        public static IEnumerable<KeyValuePair<string, InternalHandle>> GetOwnProperties(this InternalHandle value)
        {
            return value.GetProperties();
        }

        public static IEnumerable<KeyValuePair<string, InternalHandle>> GetProperties(this InternalHandle value)
        {
            var propertyNames = value.GetPropertyNames();

            foreach (var propertyName in propertyNames)
            {
                yield return new KeyValuePair<string, InternalHandle>(propertyName, value.GetProperty(propertyName));
            }
        }

        public static bool HasOwnProperty (this V8NativeObject obj, string name)
        {
            return obj._.HasOwnProperty(name);
        }

        public static bool HasProperty (this V8NativeObject obj, string name)
        {
            return obj._.HasProperty(name);
        }

        public static bool HasOwnProperty (this InternalHandle value, string name)
        {
            return value.HasProperty(name);
        }

        public static bool HasProperty (this InternalHandle value, string name)
        {
            /*var attr = value.GetPropertyAttributes(name);
            return attr != V8PropertyAttributes.Undefined;*/
            /*using (var jsHasOwn = Execute("Object.hasOwn"))
            {
                jsHasOwn.StaticCall()
            }*/
            using (var jsRes = value.GetProperty(name))
                return !jsRes.IsUndefined;
        }

        public static bool TryGetValue(this InternalHandle obj, string propertyName, out InternalHandle jsRes)
        {
            jsRes = obj.GetProperty(propertyName);
            if (jsRes.IsUndefined) {
                jsRes.Dispose();
                return false;
            }
            return true;
        }

        public static bool TryGetValue(this V8NativeObject obj, string propertyName, out InternalHandle jsRes)
        {
            return obj._.TryGetValue(propertyName, out jsRes);
        }

        public static void FastAddProperty(this V8NativeObject obj, string name, InternalHandle value, bool writable, bool enumerable, bool configurable)
        {
            obj._.FastAddProperty(name, value, writable, enumerable, configurable);
        }

        public static void FastAddProperty(this InternalHandle obj, string name, InternalHandle value, bool writable, bool enumerable, bool configurable)
        {
            if (obj.SetProperty(name, value) == false)
            {
                throw new InvalidOperationException($"Failed to fast add property {name}");
            }
        }

        /*public static IDisposable ChangeMaxStatements(this V8Engine engine, int value)
        {
            var maxStatements = engine.FindConstraint<MaxStatements>();
            if (maxStatements == null)
                return null;

            var oldMaxStatements = maxStatements.Max;
            maxStatements.Change(value);
            return new DisposableAction(maxStatements.Change(oldMaxStatements));
        }*/ 

        public static IDisposable ChangeMaxStatements(this V8Engine engine, int value)
        {
            void DoNothing()
            {}

            return new DisposableAction(DoNothing);
        }

        public static IDisposable DisableMaxStatements(this V8Engine engine)
        {
            return ChangeMaxStatements(engine, int.MaxValue);
        }

        public static void ResetCallStack(this V8Engine engine)
        {
            // TODO
        }

        public static void ResetConstraints(this V8Engine engine)
        {
            // TODO
        }

        public static void ExecuteWithReset(this V8Engine engine, string source, string sourceName = "V8.NET", bool throwExceptionOnError = true, int timeout = 0)
        {
            using (engine.ExecuteExprWithReset(source, sourceName, throwExceptionOnError, timeout))
            {}
        }

        public static void ExecuteWithReset(this V8Engine engine, InternalHandle script, bool throwExceptionOnError = true, int timeout = 0)
        {
            using (engine.ExecuteExprWithReset(script, throwExceptionOnError, timeout))
            {}
        }

        public static InternalHandle ExecuteExprWithReset(this V8Engine engine, string source, string sourceName = "V8.NET", bool throwExceptionOnError = true, int timeout = 0)
        {
            using (var script = engine.Compile(source, sourceName, throwExceptionOnError))
            {
                return ExecuteExprWithReset(engine, script, throwExceptionOnError, timeout);
            }
        }

        public static InternalHandle ExecuteExprWithReset(this V8Engine engine, InternalHandle script, bool throwExceptionOnError = true, int timeout = 0)
        {
            try
            {
                return engine.Execute(script, throwExceptionOnError, timeout);
            }
            finally
            {
                engine.ResetCallStack();
                engine.ResetConstraints();
            }
        }
    }
}
