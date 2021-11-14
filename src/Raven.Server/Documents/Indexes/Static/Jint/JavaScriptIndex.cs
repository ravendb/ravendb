using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Esprima;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Interop;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.Documents.Indexes.Static.JavaScript.Jint;
using Raven.Server.Documents.Indexes.Static.Counters.Jint;
using Raven.Server.Documents.Indexes.Static.TimeSeries.Jint;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Extensions.Jint;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.Indexes.Static.Utils;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Static
{
    public sealed partial class JavaScriptIndex
    {
        private JsValue GetDocumentIdJint(JsValue self, JsValue[] args)
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JsUtilsJint);

            return JsUtilsJint.GetDocumentId(self, args);
        }

        private JsValue AttachmentsForJint(JsValue self, JsValue[] args)
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JsUtilsJint);

            return JsUtilsJint.AttachmentsFor(self, args);
        }

        private JsValue MetadataForJint(JsValue self, JsValue[] args)
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JsUtilsJint);

            return JsUtilsJint.GetMetadata(self, args);
        }

        private JsValue TimeSeriesNamesForJint(JsValue self, JsValue[] args)
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JsUtilsJint);

            return JsUtilsJint.GetTimeSeriesNamesFor(self, args);
        }

        private JsValue CounterNamesForJint(JsValue self, JsValue[] args)
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JsUtilsJint);

            return JsUtilsJint.GetCounterNamesFor(self, args);
        }

        private JsValue LoadAttachmentJint(JsValue self, JsValue[] args)
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JsUtilsJint);

            return JsUtilsJint.LoadAttachment(self, args);
        }

        private JsValue LoadAttachmentsJint(JsValue self, JsValue[] args)
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JsUtilsJint);

            return JsUtilsJint.LoadAttachments(self, args);
        }
    }

    public abstract partial class AbstractJavaScriptIndex
    {
        public JintEngineEx EngineExJint;
        public Engine EngineJint;
        public JavaScriptUtilsJint JsUtilsJint;

        public void DisposeJint()
        {
            EngineExJint.Dispose();
        }
        
        protected void InitializeJint()
        {
            var refResolver = new JintPreventResolvingTasksReferenceResolver();
            EngineExJint = new JintEngineEx(JsOptions, refResolver);
            EngineJint = EngineExJint;
            EngineHandle = EngineExJint;
            _engineForParsing = EngineExJint;
        }

        protected void InitializeJint2()
        {
            JsUtilsJint = (JavaScriptUtilsJint)JsUtils;
        }

        public void InitializeLockedJint()
        {
        }
            
        public void InitContextJint(JavaScriptMapOperation operation)
        {
        }
            
        public void InitContextJint(JavaScriptReduceOperation operation)
        {
        }
            
        private JsValue RecurseJint(JsValue self, JsValue[] args)
        {
            if (args.Length != 2)
            {
                throw new ArgumentException("The recurse(item, func) method expects two arguments, but got: " + args.Length);
            }

            var item = args[0];
            var func = args[1] as ScriptFunctionInstance;

            if (func == null)
                throw new ArgumentException("The second argument in recurse(item, func) must be an arrow function.");

            return new RecursiveJsFunctionJint(EngineJint, item, func).Execute();
        }

        private JsValue TryConvertToNumberJint(JsValue self, JsValue[] args)
        {
            if (args.Length != 1)
            {
                throw new ArgumentException("The tryConvertToNumber(value) method expects one argument, but got: " + args.Length);
            }

            var value = args[0];

            if (value.IsNull() || value.IsUndefined())
                return DynamicJsNullJint.ImplicitNullJint;

            if (value.IsNumber())
                return value;

            if (value.IsString())
            {
                var valueAsString = value.AsString();
                if (double.TryParse(valueAsString, out var valueAsDbl))
                    return valueAsDbl;
            }

            return DynamicJsNullJint.ImplicitNullJint;
        }

        private JsValue LoadDocumentJint(JsValue self, JsValue[] args)
        {
            if (args.Length != 2)
            {
                throw new ArgumentException("The load(id, collection) method expects two arguments, but got: " + args.Length);
            }

            if (args[0].IsNull() || args[0].IsUndefined())
                return DynamicJsNullJint.ImplicitNullJint;

            if (args[0].IsString() == false ||
                args[1].IsString() == false)
            {
                throw new ArgumentException($"The load(id, collection) method expects two string arguments, but got: load({args[0]}, {args[1]})");
            }

            object doc = CurrentIndexingScope.Current.LoadDocument(null, args[0].AsString(), args[1].AsString());
            if (!(doc is DynamicNullObject) && JsIndexUtils.GetValue(doc, out var itemHandle))
                return itemHandle.Jint.Item;

            return DynamicJsNullJint.ImplicitNullJint;
        }

        private JsValue LoadCompareExchangeValueJint(JsValue self, JsValue[] args)
        {
            if (args.Length != 1)
                throw new ArgumentException("The cmpxchg(key) method expects one argument, but got: " + args.Length);

            var keyArgument = args[0];
            if (keyArgument.IsNull() || keyArgument.IsUndefined())
                return DynamicJsNullJint.ImplicitNullJint;

            if (keyArgument.IsString())
            {
                object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, keyArgument.AsString());
                return ConvertToJsValueJint(value);
            }
            else if (keyArgument.IsArray())
            {
                var keys = keyArgument.AsArray();
                if (keys.Length == 0)
                    return DynamicJsNullJint.ImplicitNullJint;

                var values = EngineJint.Realm.Intrinsics.Array.Construct(keys.Length);
                var arrayArgs = new JsValue[1];
                for (uint i = 0; i < keys.Length; i++)
                {
                    var key = keys[i];
                    if (key.IsString() == false)
                        ThrowInvalidTypeJint(key, Types.String);

                    object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, key.AsString());
                    arrayArgs[0] = ConvertToJsValueJint(value);

                    EngineJint.Realm.Intrinsics.Array.PrototypeObject.Push(values, args);
                }

                return values;
            }
            else
            {
                throw new InvalidOperationException($"Argument '{keyArgument}' was of type '{keyArgument.Type}', but either string or array of strings was expected.");
            }

            JsValue ConvertToJsValueJint(object value)
            {
                switch (value)
                {
                    case null:
                        return DynamicJsNullJint.ImplicitNullJint;

                    case DynamicNullObject dno:
                        return dno.IsExplicitNull ? DynamicJsNullJint.ExplicitNullJint : DynamicJsNullJint.ImplicitNullJint;

                    case DynamicBlittableJson dbj:
                        return new BlittableObjectInstanceJint(EngineJint, null, dbj.BlittableJson, id: null, lastModified: null, changeVector: null);

                    default:
                        return JsUtilsJint.TranslateToJs(context: null, value);
                }
            }

            static void ThrowInvalidTypeJint(JsValue value, Types expectedType)
            {
                throw new InvalidOperationException($"Argument '{value}' was of type '{value.Type}', but '{expectedType}' was expected.");
            }
        }
    }
}
