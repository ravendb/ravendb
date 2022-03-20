using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using Jint;
using Jint.Native;
using Raven.Server.Extensions.Jint;
using Raven.Server.Documents.Indexes.Static.JavaScript.Jint;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config.Categories;
using JSFunction = V8.Net.JSFunction;
using JSValueType = V8.Net.JSValueType;
using Raven.Client.Exceptions.Documents.Patching;
using Jint.Constraints;
using Raven.Client.Util;
using Raven.Server.Config.Settings;

namespace Raven.Server.Documents.Patch.Jint
{
    public class JintEngineEx : Engine, IJsEngineHandle
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

        public JsHandle ImplicitNull() => new(ImplicitNullJint);
        public JsHandle ExplicitNull() => new(ExplicitNullJint);

        private readonly JsHandle _jsonStringify;
        public JsHandle JsonStringify() => _jsonStringify;

        [CanBeNull]
        private IJavaScriptOptions _jsOptions;

        public  bool IsMemoryChecksOn => false;

        public JintEngineEx(IJavaScriptOptions jsJsOptions = null, JintPreventResolvingTasksReferenceResolver refResolver = null) : base(options =>
        {
            if (jsJsOptions == null)
                options.MaxStatements(1).LimitRecursion(1);
            else
            {
                var maxDurationMs = jsJsOptions.MaxDuration.GetValue(TimeUnit.Milliseconds);
                options.LimitRecursion(64)
                    .SetReferencesResolver(refResolver)
                    .Strict(jsJsOptions.StrictMode)
                    .MaxStatements(jsJsOptions.MaxSteps)
                    .AddObjectConverter(new JintGuidConverter())
                    .AddObjectConverter(new JintStringConverter())
                    .AddObjectConverter(new JintEnumConverter())
                    .AddObjectConverter(new JintDateTimeConverter())
                    .AddObjectConverter(new JintTimeSpanConverter())
                    .LocalTimeZone(TimeZoneInfo.Utc);

                //options.TimeoutInterval(maxDurationMs == 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(maxDurationMs)); // TODO [shlomo] to switch it on when tests get stable to exclude break because of operation timeout
            }
        })
        {
            _jsOptions = jsJsOptions;
            
            RefResolver = refResolver;

            ExecuteWithReset(ExecEnvCodeJint, "ExecEnvCode");

            _jsonStringify = new JsHandle(Evaluate("JSON.stringify"));
        }

        ~JintEngineEx() 
        {
            Dispose(false);
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
            var maxStatements = FindConstraint<MaxStatements>();
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

        public IDisposable ChangeMaxDuration(TimeSpan value)
        {
            var maxDuration = FindConstraint<TimeConstraint2>();
            if (maxDuration == null)
                return null;

            var oldMaxDuration = maxDuration.Timeout;
            maxDuration.Change(value); 

            return new DisposableAction(() => maxDuration.Change(oldMaxDuration));
        }
        
        public void ForceGarbageCollection()
        {}

        public object MakeSnapshot(string name)
        {
            return null;
        }

        public bool RemoveMemorySnapshot(string name)
        {
            return false;
        }

        public void AddToLastMemorySnapshotBefore(JsHandle h) {}
        
        public void RemoveFromLastMemorySnapshotBefore(JsHandle h) {}
        
        public void CheckForMemoryLeaks(string name, bool shouldRemove = true)
        {}

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
                base.Execute(source);
            else
            {
                try
                {
                    base.Execute(source);
                }
                catch
                {
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExecuteWithReset(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true)
        {
            this.ExecuteWithReset(source, throwExceptionOnError);
        }
        
        public JsHandle GlobalObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(Realm.GlobalObject);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetGlobalProperty(string propertyName)
        {
            return new JsHandle(GetValue(propertyName));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetGlobalProperty(string name, JsHandle value)
        {
            SetValue(name, value.Jint.Item ?? JsValue.Undefined);
        }

        public JsHandle FromObjectGen(object obj, bool keepAlive = false)
        {
            return new JsHandle(JsValue.FromObject(this, obj));
        }

        public JsHandle CreateClrCallBack(string propertyName, (Func<JsValue, JsValue[], JsValue> Jint, JSFunction V8) funcTuple, bool keepAlive = true)
        {
            return new JsHandle(this.CreateClrCallBack(propertyName, funcTuple.Jint));
        }

        public void SetGlobalClrCallBack(string propertyName, (Func<JsValue, JsValue[], JsValue> Jint, JSFunction V8) funcTuple)
        {
            this.SetGlobalClrCallBack(propertyName, funcTuple.Jint);
        }

        public JsHandle CreateObject()
        {
            return new JsHandle(Realm.Intrinsics.Object.Construct(System.Array.Empty<JsValue>())); //new ObjectInstance(this));
        }

        public JsHandle CreateEmptyArray()
        {
            var be = (Engine)this;
            return new JsHandle(be.CreateEmptyArray());
        }
        
        public JsHandle CreateArray(JsHandle[] items)
        {
            int arrayLength = items.Length;
            var jsItems = new JsValue[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                var jhItem = items[i];
                jsItems[i] = jhItem.Jint.Item;
            }
            return new JsHandle(JintExtensions.CreateArray(this, jsItems));
        }

        public JsHandle CreateArray(System.Array items)
        {
            int arrayLength = items.Length;
            var jsItems = new JsValue[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsItems[i] = this.FromObject(items.GetValue(i));
            }
            return new JsHandle(JintExtensions.CreateArray(this, jsItems));
        }

        public JsHandle CreateArray(IEnumerable<object> items)
        {
            var be = (Engine)this;
            var list = be.CreateEmptyArray();
            void PushKey(object value)
            {
                var jsValue = be.FromObject(value);
                list.AsObject().StaticCall("push", jsValue);
            }

            foreach (var item in items)
                PushKey(item);
            return new JsHandle(list);
        }

        public JsHandle CreateUndefinedValue()
        {
            return new JsHandle(JsValue.Undefined);
        }
        
        public JsHandle CreateNullValue()
        {
            return new JsHandle(JsValue.Null);
        }
        
        public JsHandle CreateValue(bool value)
        {
            return new JsHandle(new JsBoolean(value));
        }

        public JsHandle CreateValue(Int32 value)
        {
            return new JsHandle(new JsNumber(value));
        }

        public JsHandle CreateValue(double value)
        {
            return new JsHandle(new JsNumber(value));
        }

        public JsHandle CreateValue(string value)
        {
            return new JsHandle(new JsString(value));
        }

        public JsHandle CreateValue(TimeSpan ms)
        {
            var be = (Engine)this;
            return new JsHandle(be.FromObject(ms));
        }

        public JsHandle CreateValue(DateTime value)
        {
            var be = (Engine)this;
            return new JsHandle(be.FromObject(value));
        }

        public JsHandle CreateError(string message, JSValueType errorType)
        {
            return new JsHandle(message, errorType);
        }
    }
}
