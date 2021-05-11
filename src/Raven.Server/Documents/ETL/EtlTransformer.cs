using System;
using System.Collections.Generic;
using System.Linq;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlTransformer<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation> : IDisposable 
        where TExtracted : ExtractedItem
        where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
        where TEtlPerformanceOperation : EtlPerformanceOperation
    {
        public DocumentDatabase Database { get; }
        protected readonly DocumentsOperationContext Context;
        private readonly PatchRequest _mainScript;
        private readonly PatchRequest _behaviorFunctions;
        protected ScriptRunner.SingleRun DocumentScript;
        protected ScriptRunner.SingleRun BehaviorsScript;

        protected TExtracted Current;

        private ScriptRunner.ReturnRun _returnMainRun;
        private ScriptRunner.ReturnRun _behaviorFunctionsRun;

        protected EtlTransformer(DocumentDatabase database, DocumentsOperationContext context,
            PatchRequest mainScript, PatchRequest behaviorFunctions)
        {
            Database = database;
            Context = context;
            _mainScript = mainScript;
            _behaviorFunctions = behaviorFunctions;
        }

        public virtual void Initialize(bool debugMode)
        {
            if (_behaviorFunctions != null)
            {
                _behaviorFunctionsRun = Database.Scripts.GetScriptRunner(_behaviorFunctions, true, out BehaviorsScript);

                if (debugMode)
                    BehaviorsScript.DebugMode = true;
            }
            
            _returnMainRun = Database.Scripts.GetScriptRunner(_mainScript, true, out DocumentScript);
            if (DocumentScript == null)
                return;

            if (debugMode)
                DocumentScript.DebugMode = true;

            DocumentScript.ScriptEngine.SetValue(Transformation.LoadTo, new ClrFunctionInstance(DocumentScript.ScriptEngine, Transformation.LoadTo, LoadToFunctionTranslator));

            foreach (var collection in LoadToDestinations)
            {
                var name = Transformation.LoadTo + collection;
                DocumentScript.ScriptEngine.SetValue(name, new ClrFunctionInstance(DocumentScript.ScriptEngine, name, 
                    (value, values) => LoadToFunctionTranslator(collection, value, values)));
            }

            DocumentScript.ScriptEngine.SetValue(Transformation.LoadAttachment, new ClrFunctionInstance(DocumentScript.ScriptEngine, Transformation.LoadAttachment, LoadAttachment));

            const string loadCounter = Transformation.CountersTransformation.Load;
            DocumentScript.ScriptEngine.SetValue(loadCounter, new ClrFunctionInstance(DocumentScript.ScriptEngine, loadCounter, LoadCounter));

            const string loadTimeSeries = Transformation.TimeSeriesTransformation.LoadTimeSeries.Name;
            DocumentScript.ScriptEngine.SetValue(loadTimeSeries, new ClrFunctionInstance(DocumentScript.ScriptEngine, loadTimeSeries, LoadTimeSeries));

            DocumentScript.ScriptEngine.SetValue("getAttachments", new ClrFunctionInstance(DocumentScript.ScriptEngine, "getAttachments", GetAttachments));

            DocumentScript.ScriptEngine.SetValue("hasAttachment", new ClrFunctionInstance(DocumentScript.ScriptEngine, "hasAttachment", HasAttachment));

            DocumentScript.ScriptEngine.SetValue("getCounters", new ClrFunctionInstance(DocumentScript.ScriptEngine, "getCounters", GetCounters));

            DocumentScript.ScriptEngine.SetValue("hasCounter", new ClrFunctionInstance(DocumentScript.ScriptEngine, "hasCounter", HasCounter));
            
            const string hasTimeSeries = Transformation.TimeSeriesTransformation.HasTimeSeries.Name;
            DocumentScript.ScriptEngine.SetValue(hasTimeSeries, new ClrFunctionInstance(DocumentScript.ScriptEngine, hasTimeSeries, HasTimeSeries));
            
            const string getTimeSeries = Transformation.TimeSeriesTransformation.GetTimeSeries.Name;
            DocumentScript.ScriptEngine.SetValue(getTimeSeries, new ClrFunctionInstance(DocumentScript.ScriptEngine, getTimeSeries, GetTimeSeries));
        }

        private JsValue LoadToFunctionTranslator(JsValue self, JsValue[] args)
        {
            if (args.Length != 2)
                ThrowInvalidScriptMethodCall("loadTo(name, obj) must be called with exactly 2 parameters");
            if (args[0].IsString() == false)
                ThrowInvalidScriptMethodCall("loadTo(name, obj) first argument must be a string");
            if (args[1].IsObject() == false)
                ThrowInvalidScriptMethodCall("loadTo(name, obj) second argument must be an object");

            // explicitly not disposing here, this will clear the context from the JavaScriptUtils, but this is 
            // called _midway_ through the script, so that is not something that we want to do. The caller will
            // already be calling that.
            var result = new ScriptRunnerResult(DocumentScript, args[1].AsObject());
            LoadToFunction(args[0].AsString(), result);
            return result.Instance;
        }

        private JsValue LoadToFunctionTranslator(string name, JsValue self, JsValue[] args)
        {
            if (args.Length != 1)
                ThrowInvalidScriptMethodCall($"loadTo{name}(obj) must be called with exactly 1 parameter");

            if (args[0].IsObject() == false)
                ThrowInvalidScriptMethodCall($"loadTo{name}(obj) argument must be an object");

            // explicitly not disposing here, this will clear the context from the JavaScriptUtils, but this is 
            // called _midway_ through the script, so that is not something that we want to do. The caller will
            // already be calling that.
            var result = new ScriptRunnerResult(DocumentScript, args[0].AsObject());
            LoadToFunction(name, result);
            return result.Instance;
        }

        protected abstract void AddLoadedAttachment(JsValue reference, string name, Attachment attachment);

        protected abstract void AddLoadedCounter(JsValue reference, string name, long value);
        
        protected abstract void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries);

        private JsValue LoadAttachment(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidScriptMethodCall($"{Transformation.LoadAttachment}(name) must have a single string argument");

            var attachmentName = args[0].AsString();
            var loadAttachmentReference = CreateLoadAttachmentReference(attachmentName);

            if ((Current.Document.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
            {
                var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(Context, Current.DocumentId, attachmentName, AttachmentType.Document, null);

                if (attachment == null)
                    return JsValue.Null;

                AddLoadedAttachment(loadAttachmentReference, attachmentName, attachment);
            }
            else
            {
                return JsValue.Null;
            }

            return loadAttachmentReference;
        }

        private static JsValue CreateLoadAttachmentReference(string attachmentName)
        {
            return $"{Transformation.AttachmentMarker}{attachmentName}{Guid.NewGuid():N}";
        }

        private JsValue LoadCounter(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidScriptMethodCall($"{Transformation.CountersTransformation.Load}(name) must have a single string argument");

            var counterName = args[0].AsString();
            var loadCounterReference = CreateLoadCounterReference(counterName);

            if ((Current.Document.Flags & DocumentFlags.HasCounters) == DocumentFlags.HasCounters)
            {
                var value = Database.DocumentsStorage.CountersStorage.GetCounterValue(Context, Current.DocumentId, counterName);

                if (value == null)
                    return JsValue.Null;

                AddLoadedCounter(loadCounterReference, counterName, value.Value.Value);
            }
            else
            {
                return JsValue.Null;
            }

            return loadCounterReference;
        }

        private static JsValue CreateLoadCounterReference(string counterName)
        {
            return Transformation.CountersTransformation.Marker + counterName;
        }

        private JsValue LoadTimeSeries(JsValue self, JsValue[] args)
        {
            if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) == DocumentFlags.HasTimeSeries == false)
                return JsValue.Null;

            const int minParamsCount = Transformation.TimeSeriesTransformation.LoadTimeSeries.MinParamsCount;
            const int maxParamsCount = Transformation.TimeSeriesTransformation.LoadTimeSeries.MaxParamsCount;
            const string signature = Transformation.TimeSeriesTransformation.LoadTimeSeries.Signature;
            
            if (args.Length < minParamsCount || args.Length > maxParamsCount)
                ThrowInvalidScriptMethodCall($"{signature} must have between {minParamsCount} to {maxParamsCount} arguments");
                
            if(args[0].IsString() == false)
                ThrowInvalidScriptMethodCall($"{signature}. The argument timeSeriesName must be a string");
            var timeSeriesName = args[0].AsString();

            var from = args.Length < 2 ? DateTime.MinValue : ScriptRunner.GetDateArg(args[1], signature, "from"); 
            var to = args.Length < 3 ? DateTime.MaxValue : ScriptRunner.GetDateArg(args[2], signature, "to"); 
                
            var loadTimeSeriesReference = CreateLoadTimeSeriesReference(timeSeriesName, @from, to);

            var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(Context, Current.DocumentId, timeSeriesName, from, to);
            if(reader.AllValues().Any() == false)
                return JsValue.Null;

            AddLoadedTimeSeries(loadTimeSeriesReference, timeSeriesName, reader.AllValues());

            return loadTimeSeriesReference;
        }

        private static JsValue CreateLoadTimeSeriesReference(string timeSeriesName, DateTime from, DateTime to)
        {
            return Transformation.TimeSeriesTransformation.Marker + timeSeriesName + from.Ticks + ':' + to.Ticks;
        }

        private JsValue GetAttachments(JsValue self, JsValue[] args)
        {
            if (args.Length != 0)
                ThrowInvalidScriptMethodCall("getAttachments() must be called without any argument");

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachmentsBlittableArray) == false)
            {
                return DocumentScript.ScriptEngine.Array.Construct(Array.Empty<JsValue>());
            }

            var attachments = new JsValue[attachmentsBlittableArray.Length];

            for (int i = 0; i < attachmentsBlittableArray.Length; i++)
            {
                attachments[i] = (JsValue)DocumentScript.Translate(Context, attachmentsBlittableArray[i]);
            }

            return DocumentScript.ScriptEngine.Array.Construct(attachments);
        }

        private JsValue HasAttachment(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidScriptMethodCall("hasAttachment(name) must be called with one argument (string)");

            if ((Current.Document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments)
                return false;

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
            {
                return false;
            }

            var checkedName = args[0].AsString();

            foreach (var attachment in attachments)
            {
                var attachmentInfo = (BlittableJsonReaderObject)attachment;
                
                if (attachmentInfo.TryGet(nameof(AttachmentName.Name), out string name) && checkedName.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        private JsValue GetCounters(JsValue self, JsValue[] args)
        {
            if (args.Length != 0)
                ThrowInvalidScriptMethodCall("getCounters() must be called without any argument");

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray countersArray) == false)
            {
                return DocumentScript.ScriptEngine.Array.Construct(Array.Empty<JsValue>());
            }

            var counters = new JsValue[countersArray.Length];

            for (int i = 0; i < countersArray.Length; i++)
            {
                counters[i] = (JsValue)DocumentScript.Translate(Context, countersArray[i]);
            }

            return DocumentScript.ScriptEngine.Array.Construct(counters);
        }

        private JsValue HasCounter(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidScriptMethodCall("hasCounter(name) must be called with one argument (string)");

            if ((Current.Document.Flags & DocumentFlags.HasCounters) != DocumentFlags.HasCounters)
                return false;

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters) == false)
            {
                return false;
            }

            var checkedName = args[0].AsString();

            foreach (var counter in counters)
            {
                var counterName = (LazyStringValue)counter;

                if (checkedName.Equals(counterName, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private JsValue GetTimeSeries(JsValue self, JsValue[] args)
        {
            const int paramsCount = Transformation.TimeSeriesTransformation.GetTimeSeries.ParamsCount;
            const string signature = Transformation.TimeSeriesTransformation.GetTimeSeries.Signature;
            
            if (args.Length != paramsCount)
                ThrowInvalidScriptMethodCall($"{signature} must be called without any argument");

            if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) != DocumentFlags.HasTimeSeries)
                return false;
            
            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeriesArray) == false)
            {
                return DocumentScript.ScriptEngine.Array.Construct(Array.Empty<JsValue>());
            }

            var timeSeriesNames = new JsValue[timeSeriesArray.Length];
            for (int i = 0; i < timeSeriesArray.Length; i++)
            {
                timeSeriesNames[i] = (JsValue)DocumentScript.Translate(Context, timeSeriesArray[i]);
            }
            return DocumentScript.ScriptEngine.Array.Construct(timeSeriesNames);
        }

        private JsValue HasTimeSeries(JsValue self, JsValue[] args)
        {
            const int paramsCount = Transformation.TimeSeriesTransformation.HasTimeSeries.ParamsCount;
            const string signature = Transformation.TimeSeriesTransformation.HasTimeSeries.Signature;

            if (args.Length != paramsCount || args[0].IsString() == false)
                ThrowInvalidScriptMethodCall($"{signature} must be called with one argument (string)");

            if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) != DocumentFlags.HasTimeSeries)
                return false;

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeriesNames) == false)
            {
                return false;
            }

            var checkedName = args[0].AsString();

            foreach (var timeSeries in timeSeriesNames)
            {
                var counterName = (LazyStringValue)timeSeries;
                if (checkedName.Equals(counterName, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }
        
        protected abstract string[] LoadToDestinations { get; }

        protected abstract void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject);

        public abstract IEnumerable<TTransformed> GetTransformedResults();

        public abstract void Transform(TExtracted item, TStatsScope stats, EtlProcessState state);

        public static void ThrowLoadParameterIsMandatory(string parameterName)
        {
            throw new ArgumentException($"{parameterName} parameter is mandatory");
        }

        protected static void ThrowInvalidScriptMethodCall(string message)
        {
            throw new InvalidOperationException(message);
        }

        public void Dispose()
        {
            using (_returnMainRun)
            using (_behaviorFunctionsRun)
            {

            }
        }

        public List<string> GetDebugOutput()
        {
            var outputs = new List<string>();

            if (DocumentScript?.DebugOutput != null)
                outputs.AddRange(DocumentScript.DebugOutput);

            if (BehaviorsScript?.DebugOutput != null)
                outputs.AddRange(BehaviorsScript.DebugOutput);

            return outputs;
        }
    }
}
