using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using JetBrains.Annotations;
using Jint;
using Jint.Constraints;
using Jint.Native;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.ServerWide.JavaScript;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Static.JavaScript.Jint;
using Raven.Server.Extensions.Jint;
using Sparrow.Json;
using JSValueType = V8.Net.JSValueType;

namespace Raven.Server.Documents.Patch.Jint
{
    public class JintEngineEx : IJsEngineHandle<JsHandleJint>, IDisposable
    {

        public const string ExecEnvCodeJint = @"
var process = {
    env: {
        EXEC_ENV: 'RavenDB',
        ENGINE: 'Jint'
    }
}
";
        public readonly JintPreventResolvingTasksReferenceResolver RefResolver;

        public DynamicJsNullJint ImplicitNullJint;
        public DynamicJsNullJint ExplicitNullJint;


        //public JsHandleJint ImplicitNull() => new(ImplicitNullJint);
        //public JsHandleJint ExplicitNull() => new(ExplicitNullJint);

        private readonly JsHandleJint _jsonStringify;
        public JsHandleJint JsonStringify() => _jsonStringify;

        [CanBeNull]
        private IJavaScriptOptions _jsOptions;

        public bool IsMemoryChecksOn => false;
        public JsHandleJint Empty { get; set; } = JsHandleJint.Empty;
        public JsHandleJint Null { get; set; } = JsHandleJint.Null;
        public JsHandleJint Undefined { get; set; } = JsHandleJint.Undefined;
        public JsHandleJint True { get; set; } = JsHandleJint.True;
        public JsHandleJint False { get; set; } = JsHandleJint.False;
        public JsHandleJint ImplicitNull { get; set; } = JsHandleJint.ImplicitNull;
        public JsHandleJint ExplicitNull { get; set; } = JsHandleJint.ExplicitNull;

        public readonly Engine Engine;

        public JintEngineEx(RavenConfiguration configuration, JintPreventResolvingTasksReferenceResolver refResolver = null)
        {
            var jsOptions = configuration.JavaScript;
            Engine = new Engine(options =>
            {
                if (jsOptions == null)
                    options.MaxStatements(1).LimitRecursion(1);
                else
                {
                    var maxDurationMs = jsOptions.MaxDuration.GetValue(TimeUnit.Milliseconds);
                    options.LimitRecursion(64)
                        .MaxStatements(jsOptions.MaxSteps)
                        .Strict(jsOptions.StrictMode)
                        .AddObjectConverter(new JintGuidConverter())
                        .AddObjectConverter(new JintStringConverter())
                        .AddObjectConverter(new JintEnumConverter())
                        .AddObjectConverter(new JintDateTimeConverter())
                        .AddObjectConverter(new JintTimeSpanConverter())
                        .LocalTimeZone(TimeZoneInfo.Utc);

                    if (refResolver != null)
                    {
                        options.SetReferencesResolver(refResolver);
                    }

                    //options.TimeoutInterval(maxDurationMs == 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(maxDurationMs)); // TODO [shlomo] to switch it on when tests get stable to exclude break because of operation timeout
                }
            });
            _jsOptions = jsOptions;

            RefResolver = refResolver;

            ExecuteWithReset(ExecEnvCodeJint, "ExecEnvCode");

            _jsonStringify = new JsHandleJint(Engine.Evaluate("JSON.stringify"));
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Dispose(bool disposing)
        {
            _jsonStringify.Dispose();
        }

        public void SetBasicConfiguration()
        {
            //.LocalTimeZone(TimeZoneInfo.Utc);  // TODO -> ??? maybe these V8 args: harmony_intl_locale_info, harmony_intl_more_timezone
        }

        // ------------------------------------------ IJavaScriptEngineHandle implementation
        public JavaScriptEngineType EngineType => JavaScriptEngineType.Jint;

        [CanBeNull]
        public IJavaScriptOptions JsOptions => _jsOptions;

        public IDisposable DisableConstraints()
        {
            var disposeMaxStatements = ChangeMaxStatements(0);
            var disposeMaxDuration = ChangeMaxDuration(0);

            void Restore()
            {
                disposeMaxStatements?.Dispose();
                disposeMaxDuration?.Dispose();
            }

            return new DisposableAction(Restore);
        }

        public IDisposable ChangeMaxStatements(int value)
        {
            var maxStatements = Engine.FindConstraint<MaxStatements>();
            if (maxStatements == null)
                return null;

            var oldMaxStatements = maxStatements.Max;
            maxStatements.Change(value == 0 ? int.MaxValue : value);

            return new DisposableAction(() => maxStatements.Change(oldMaxStatements));
        }

        public IDisposable ChangeMaxDuration(int value)
        {
            return ChangeMaxDuration(value == 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(value));
        }

        public void ResetCallStack()
        {
            Engine.ResetCallStack();
        }

        public void ResetConstraints()
        {
            Engine.ResetConstraints();
        }

        public IDisposable ChangeMaxDuration(TimeSpan value)
        {
            var maxDuration = Engine.FindConstraint<TimeConstraint2>();
            if (maxDuration == null)
                return null;

            var oldMaxDuration = maxDuration.Timeout;
            maxDuration.Change(value);

            return new DisposableAction(() => maxDuration.Change(oldMaxDuration));
        }

        public void ForceGarbageCollection()
        { }

        public object MakeSnapshot(string name)
        {
            return null;
        }

        public bool RemoveMemorySnapshot(string name)
        {
            return false;
        }

        public void AddToLastMemorySnapshotBefore(JsHandleJint h) { }

        public void RemoveFromLastMemorySnapshotBefore(JsHandleJint h) { }

        public void CheckForMemoryLeaks(string name, bool shouldRemove = true)
        { }

        public void TryCompileScript(string script)
        {
            try
            {
                Execute(script);
            }
            catch (Exception e)
            {
                throw new JavaScriptParseException("Failed to parse:" + Environment.NewLine + script, e);
            }

        }

        public void Execute(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true)
        {
            if (throwExceptionOnError)
                Engine.Execute(source);
            else
            {
                try
                {
                    Engine.Execute(source);
                }
                catch
                {
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExecuteWithReset(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true)
        {
            Engine.ExecuteWithReset(source, throwExceptionOnError);
        }

        public JsHandleJint GlobalObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Engine.Realm.GlobalObject);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleJint GetGlobalProperty(string propertyName)
        {
            var prop = Engine.GetValue(propertyName);
            return new JsHandleJint(prop);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetGlobalProperty(string name, JsHandleJint value)
        {
            Engine.SetValue(name, value.Item ?? JsValue.Undefined);
        }

        public JsHandleJint FromObjectGen(object obj, bool keepAlive = false)
        {
            Debug.Assert(AssertFromObjectGen(obj), $"FromObjectGen should not receive IEnumerable, use CreateArray() instead!");
            return new JsHandleJint(JsValue.FromObject(Engine, obj));
        }

        internal static bool AssertFromObjectGen(object obj)
        {
            if (obj is IEnumerable)
            {
                if (obj is LazyStringValue)
                    return true;

                return false;
            }

            return true;
        }

        public JsHandleJint CreateClrCallBack(string propertyName, Func<JsHandleJint, JsHandleJint[], JsHandleJint> func, bool keepAlive = true)
        {
            var res = Engine.CreateClrCallBack(propertyName, CallbackFunction);

            return new JsHandleJint(res);

            JsValue CallbackFunction(JsValue arg1, JsValue[] arg2)
            {
                return func(new JsHandleJint(arg1), arg2.ToJsHandleArray()).Item;
            }
        }

        public void SetGlobalClrCallBack(string propertyName, Func<JsHandleJint, JsHandleJint[], JsHandleJint> funcTuple)
        {
            Engine.SetGlobalClrCallBack(propertyName, CallbackFunction);

            JsValue CallbackFunction(JsValue arg1, JsValue[] arg2)
            {
                return funcTuple(new JsHandleJint(arg1), arg2.ToJsHandleArray()).Item;
            }
        }

        public JsHandleJint CreateObject()
        {
            return new JsHandleJint(Engine.Realm.Intrinsics.Object.Construct(System.Array.Empty<JsValue>())); //new ObjectInstance(this));
        }

        public JsHandleJint CreateEmptyArray()
        {
            return new JsHandleJint(Engine.CreateEmptyArray());
        }

        public JsHandleJint CreateArray(JsHandleJint[] items)
        {
            int arrayLength = items.Length;
            var jsItems = new JsValue[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                var jhItem = items[i];
                jsItems[i] = jhItem.Item;
            }
            return new JsHandleJint(JintExtensions.CreateArray(Engine, jsItems));
        }

        public JsHandleJint CreateArray(IEnumerable<JsHandleJint> items)
        {
            var empty = true;
            List<JsValue> jsValues = new List<JsValue>();
            foreach (var item in items)
            {
                empty = false;
                jsValues.Add(item.Item);
            }
            if (empty)
                return CreateEmptyArray();

            var jsValue = JintExtensions.CreateArray(Engine, jsValues.ToArray());
            var jsHandleJint = new JsHandleJint(jsValue);
            return jsHandleJint;
        }

        public JsHandleJint CreateArray(IEnumerable<object> items)
        {
            var list = Engine.CreateEmptyArray();
            void PushKey(object value)
            {
                var jsValue = Engine.FromObject(value);
                list.AsObject().StaticCall("push", jsValue);
            }

            foreach (var item in items)
                PushKey(item);
            return new JsHandleJint(list);
        }

        public JsHandleJint CreateUndefinedValue()
        {
            return new JsHandleJint(JsValue.Undefined);
        }

        public JsHandleJint CreateNullValue()
        {
            return new JsHandleJint(JsValue.Null);
        }

        public JsHandleJint CreateValue(bool value)
        {
            return new JsHandleJint(new JsBoolean(value));
        }

        public JsHandleJint CreateValue(Int32 value)
        {
            return new JsHandleJint(new JsNumber(value));
        }
        public JsHandleJint CreateValue(long value)
        {
            return new JsHandleJint(new JsNumber(value));
        }

        public JsHandleJint CreateValue(double value)
        {
            return new JsHandleJint(new JsNumber(value));
        }

        public JsHandleJint CreateValue(string value)
        {
            return new JsHandleJint(new JsString(value));
        }

        public JsHandleJint CreateValue(TimeSpan ms)
        {

            return new JsHandleJint(Engine.FromObject(ms));
        }

        public JsHandleJint CreateValue(DateTime value)
        {

            return new JsHandleJint(Engine.FromObject(value));
        }

        public JsHandleJint CreateError(Exception e, JSValueType errorType)
        {
            throw e;
        }
    }
}
