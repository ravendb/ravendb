using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlTransformer<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation, T> : IDisposable 
        where TExtracted : ExtractedItem
        where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
        where TEtlPerformanceOperation : EtlPerformanceOperation
        where T : struct, IJsHandle<T>
    {
        public IJsEngineHandle<T> BehaviorsEngineHandle;
        public IJsEngineHandle<T> DocumentEngineHandle;

        public DocumentDatabase Database { get; }
        protected readonly DocumentsOperationContext Context;
        protected readonly PatchRequest _mainScript;
        protected readonly PatchRequest _behaviorFunctions;
        protected SingleRun<T> DocumentScript;
        protected SingleRun<T> BehaviorsScript;

        protected TExtracted Current;

        protected ReturnRun _returnMainRun;
        protected ReturnRun _behaviorFunctionsRun;

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
            if (debugMode)
            {
                if (BehaviorsScript != null)
                    BehaviorsScript.DebugMode = true;
                if (DocumentScript != null)
                    DocumentScript.DebugMode = true;
            }

            DocumentEngineHandle = DocumentScript?.ScriptEngineHandle;
            BehaviorsEngineHandle = BehaviorsScript?.ScriptEngineHandle;

            if (DocumentEngineHandle != null)
            {
                lock (DocumentEngineHandle)
                {
                    DocumentScript.SetContext();

                    DocumentEngineHandle.SetGlobalClrCallBack(Transformation.LoadAttachment, LoadAttachment);
                    DocumentEngineHandle.SetGlobalClrCallBack(Transformation.CountersTransformation.Load, LoadCounter);
                    DocumentEngineHandle.SetGlobalClrCallBack(Transformation.TimeSeriesTransformation.LoadTimeSeries.Name, LoadTimeSeries);
                    DocumentEngineHandle.SetGlobalClrCallBack("getAttachments", GetAttachments);
                    DocumentEngineHandle.SetGlobalClrCallBack("hasAttachment", HasAttachment);
                    DocumentEngineHandle.SetGlobalClrCallBack("getCounters", GetCounters);
                    DocumentEngineHandle.SetGlobalClrCallBack("hasCounter", HasCounter);
                    DocumentEngineHandle.SetGlobalClrCallBack(Transformation.TimeSeriesTransformation.HasTimeSeries.Name, HasTimeSeries);
                    DocumentEngineHandle.SetGlobalClrCallBack(Transformation.TimeSeriesTransformation.GetTimeSeries.Name, GetTimeSeries);

                    DocumentEngineHandle.SetGlobalClrCallBack(Transformation.LoadTo, LoadToFunctionTranslator);

                    foreach (var collection in LoadToDestinations)
                    {
                        DocumentEngineHandle.SetGlobalClrCallBack($"{Transformation.LoadTo}{collection}", (value, values) => LoadToFunctionTranslator(collection, value, values));
                    }

                    DocumentScript.ExecuteScriptsSource();
                }
            }

            //TODO: egor redundant?
            //if (BehaviorsEngineHandle != null)
            //{
            //    lock (BehaviorsEngineHandle)
            //    {
            //        BehaviorsScript.SetContext();
                    
            //        foreach (var collection in LoadToDestinations)
            //        {
            //            var name = Transformation.LoadTo + collection;
            //            BehaviorsEngineHandle.SetGlobalClrCallBack(name, (StubJint, StubV8));
            //        }

            //        BehaviorsEngineHandle.SetGlobalClrCallBack(loadAttachment, (StubJint, StubV8));
            //        BehaviorsEngineHandle.SetGlobalClrCallBack(loadCounter, (StubJint, StubV8));
            //        BehaviorsEngineHandle.SetGlobalClrCallBack(loadTimeSeries, (StubJint, StubV8));
            //        BehaviorsEngineHandle.SetGlobalClrCallBack(getAttachments, (StubJint, StubV8));
            //        BehaviorsEngineHandle.SetGlobalClrCallBack(hasAttachment, (StubJint, StubV8));
            //        BehaviorsEngineHandle.SetGlobalClrCallBack(getCounters, (StubJint, StubV8));
            //        BehaviorsEngineHandle.SetGlobalClrCallBack(hasCounter, (StubJint, StubV8));
            //        BehaviorsEngineHandle.SetGlobalClrCallBack(hasTimeSeries, (StubJint, StubV8));
            //        BehaviorsEngineHandle.SetGlobalClrCallBack(getTimeSeries, (StubJint, StubV8));

            //        BehaviorsScript.ExecuteScriptsSource();
            //    }
            //}
        }
        private T LoadToFunctionTranslator(T self, T[] args)
        {
            if (args.Length != 2)
                ThrowInvalidScriptMethodCall("loadTo(name, obj) must be called with exactly 2 parameters");
            if (args[0].IsStringEx == false)
                ThrowInvalidScriptMethodCall("loadTo(name, obj) first argument must be a string");
            if (args[1].IsObject == false)
                ThrowInvalidScriptMethodCall("loadTo(name, obj) second argument must be an object");

            // explicitly not disposing here, this will clear the context from the JavaScriptUtils, but this is 
            // called _midway_ through the script, so that is not something that we want to do. The caller will
            // already be calling that.


            var result = CreateScriptRunnerResult(args[1].AsObject());
            LoadToFunction(args[0].AsString, result);
            return result.Instance;
        }

        protected abstract ScriptRunnerResult<T> CreateScriptRunnerResult(object obj);
        private T LoadToFunctionTranslator(string name, T self, T[] args)
        {
            if (args.Length != 1)
                ThrowInvalidScriptMethodCall($"loadTo{name}(obj) must be called with exactly 1 parameter");

            if (args[0].IsObject == false)
                ThrowInvalidScriptMethodCall($"loadTo{name}(obj) argument must be an object");

            // explicitly not disposing here, this will clear the context from the JavaScriptUtils, but this is 
            // called _midway_ through the script, so that is not something that we want to do. The caller will
            // already be calling that.

            var result = CreateScriptRunnerResult(args[0].AsObject());
            LoadToFunction(name, result);
            return result.Instance;
        }

        private T LoadAttachment(T self, T[] args)
        {
            if (args.Length != 1 || args[0].IsStringEx == false)
                ThrowInvalidScriptMethodCall($"{Transformation.LoadAttachment}(name) must have a single string argument");

            var attachmentName = args[0].AsString;
            var loadAttachmentReference = CreateLoadAttachmentReference(attachmentName);

            if ((Current.Document.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
            {
                var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(Context, Current.DocumentId, attachmentName, AttachmentType.Document, null);

                if (attachment == null)
                    return DocumentEngineHandle.Null;

                AddLoadedAttachment(loadAttachmentReference, attachmentName, attachment);
            }
            else
            {
                return DocumentEngineHandle.Null;
            }

            return loadAttachmentReference;
        }

        private  T CreateLoadAttachmentReference(string attachmentName)
        {
            return DocumentEngineHandle.CreateValue($"{Transformation.AttachmentMarker}{attachmentName}{Guid.NewGuid():N}");
        }

        private T LoadCounter(T self, T[] args)
        {
            if (args.Length != 1 || args[0].IsStringEx == false)
                ThrowInvalidScriptMethodCall($"{Transformation.CountersTransformation.Load}(name) must have a single string argument");

            var counterName = args[0].AsString;
            var loadCounterReference = CreateLoadCounterReference(counterName);

            if ((Current.Document.Flags & DocumentFlags.HasCounters) == DocumentFlags.HasCounters)
            {
                var value = Database.DocumentsStorage.CountersStorage.GetCounterValue(Context, Current.DocumentId, counterName);

                if (value == null)
                    return DocumentEngineHandle.Null;

                AddLoadedCounter(loadCounterReference, counterName, value.Value.Value);
            }
            else
            {
                return DocumentEngineHandle.Null;
            }

            return loadCounterReference;
        }

        private T CreateLoadCounterReference(string counterName)
        {
            return DocumentEngineHandle.CreateValue(Transformation.CountersTransformation.Marker + counterName);
        }

        private T LoadTimeSeries(T self, T[] args)
        {
            if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) == DocumentFlags.HasTimeSeries == false)
                return DocumentEngineHandle.Null;

            const int minParamsCount = Transformation.TimeSeriesTransformation.LoadTimeSeries.MinParamsCount;
            const int maxParamsCount = Transformation.TimeSeriesTransformation.LoadTimeSeries.MaxParamsCount;
            const string signature = Transformation.TimeSeriesTransformation.LoadTimeSeries.Signature;

            if (args.Length < minParamsCount || args.Length > maxParamsCount)
                ThrowInvalidScriptMethodCall($"{signature} must have between {minParamsCount} to {maxParamsCount} arguments");

            if (args[0].IsStringEx == false)
                ThrowInvalidScriptMethodCall($"{signature}. The argument timeSeriesName must be a string");
            var timeSeriesName = args[0].AsString;

            var from = args.Length < 2 ? DateTime.MinValue : ScriptRunner<T>.GetDateArg(args[1], signature, "from");
            var to = args.Length < 3 ? DateTime.MaxValue : ScriptRunner<T>.GetDateArg(args[2], signature, "to");

            var loadTimeSeriesReference = CreateLoadTimeSeriesReference(timeSeriesName, @from, to);

            var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(Context, Current.DocumentId, timeSeriesName, from, to);
            if (reader.AllValues().Any() == false)
                return DocumentEngineHandle.Null;

            AddLoadedTimeSeries(loadTimeSeriesReference, timeSeriesName, reader.AllValues());

            return loadTimeSeriesReference;
        }

        private  T CreateLoadTimeSeriesReference(string timeSeriesName, DateTime from, DateTime to)
        {
            return DocumentEngineHandle.CreateValue(Transformation.TimeSeriesTransformation.Marker + timeSeriesName + from.Ticks + ':' + to.Ticks);
        }

        private T GetAttachments(T self, T[] args)
        {
            if (args.Length != 0)
                ThrowInvalidScriptMethodCall("getAttachments() must be called without any argument");

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachmentsBlittableArray) == false)
            {
                return DocumentEngineHandle.CreateEmptyArray();
            }

            var attachments = new T[attachmentsBlittableArray.Length];

            for (int i = 0; i < attachmentsBlittableArray.Length; i++)
            {
                attachments[i] = (T)DocumentScript.Translate(Context, attachmentsBlittableArray[i]);
            }
            
            return DocumentEngineHandle.CreateArray(attachments);
        }

        private T HasAttachment(T self, T[] args)
        {
            if (args.Length != 1 || args[0].IsStringEx == false)
                ThrowInvalidScriptMethodCall("hasAttachment(name) must be called with one argument (string)");

            if ((Current.Document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments)
                return DocumentEngineHandle.CreateValue(false);

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
            {
                return DocumentEngineHandle.CreateValue(false);
            }

            var checkedName = args[0].AsString;

            foreach (var attachment in attachments)
            {
                var attachmentInfo = (BlittableJsonReaderObject)attachment;

                if (attachmentInfo.TryGet(nameof(AttachmentName.Name), out string name) && checkedName.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return DocumentEngineHandle.CreateValue(true);
                }
            }

            return DocumentEngineHandle.CreateValue(false);
        }

        private T GetCounters(T self, T[] args)
        {
            if (args.Length != 0)
                ThrowInvalidScriptMethodCall("getCounters() must be called without any argument");

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray countersArray) == false)
            {
                return DocumentEngineHandle.CreateEmptyArray();
            }

            var counters = new T[countersArray.Length];

            for (int i = 0; i < countersArray.Length; i++)
            {
                counters[i] = (T)DocumentScript.Translate(Context, countersArray[i]);
            }

            return DocumentEngineHandle.CreateArray(counters);
        }

        private T HasCounter(T self, T[] args)
        {
            if (args.Length != 1 || args[0].IsStringEx == false)
                ThrowInvalidScriptMethodCall("hasCounter(name) must be called with one argument (string)");

            if ((Current.Document.Flags & DocumentFlags.HasCounters) != DocumentFlags.HasCounters)
                return DocumentEngineHandle.CreateValue(false);

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters) == false)
            {
                return DocumentEngineHandle.CreateValue(false);
            }

            var checkedName = args[0].AsString;

            foreach (var counter in counters)
            {
                var counterName = (LazyStringValue)counter;

                if (checkedName.Equals(counterName, StringComparison.OrdinalIgnoreCase))
                    return DocumentEngineHandle.CreateValue(true);
            }

            return DocumentEngineHandle.CreateValue(false);
        }

        private T GetTimeSeries(T self, T[] args)
        {
            const int paramsCount = Transformation.TimeSeriesTransformation.GetTimeSeries.ParamsCount;
            const string signature = Transformation.TimeSeriesTransformation.GetTimeSeries.Signature;

            if (args.Length != paramsCount)
                ThrowInvalidScriptMethodCall($"{signature} must be called without any argument");

            if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) != DocumentFlags.HasTimeSeries)
                return DocumentEngineHandle.CreateValue(false);

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeriesArray) == false)
            {
                return DocumentEngineHandle.CreateEmptyArray();
            }

            var timeSeriesNames = new T[timeSeriesArray.Length];
            for (int i = 0; i < timeSeriesArray.Length; i++)
            {
                timeSeriesNames[i] = (T)DocumentScript.Translate(Context, timeSeriesArray[i]);
            }
            return DocumentEngineHandle.CreateArray(timeSeriesNames);
        }

        private T HasTimeSeries(T self, T[] args)
        {
            const int paramsCount = Transformation.TimeSeriesTransformation.HasTimeSeries.ParamsCount;
            const string signature = Transformation.TimeSeriesTransformation.HasTimeSeries.Signature;

            if (args.Length != paramsCount || args[0].IsStringEx == false)
                ThrowInvalidScriptMethodCall($"{signature} must be called with one argument (string)");

            if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) != DocumentFlags.HasTimeSeries)
                return DocumentEngineHandle.CreateValue(false);

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeriesNames) == false)
            {
                return DocumentEngineHandle.CreateValue(false);
            }

            var checkedName = args[0].AsString;

            foreach (var timeSeries in timeSeriesNames)
            {
                var counterName = (LazyStringValue)timeSeries;
                if (checkedName.Equals(counterName, StringComparison.OrdinalIgnoreCase))
                    return DocumentEngineHandle.CreateValue(true);
            }

            return DocumentEngineHandle.CreateValue(false);
        }

        protected abstract string[] LoadToDestinations { get; }
        protected abstract void AddLoadedAttachment(T reference, string name, Attachment attachment);
        protected abstract void AddLoadedCounter(T reference, string name, long value);
        protected abstract void AddLoadedTimeSeries(T reference, string name, IEnumerable<SingleResult> entries);
        protected abstract void LoadToFunction(string tableName, ScriptRunnerResult<T> colsAsObject);

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

        public virtual void Dispose()
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

    public abstract class EtlTransformerJint<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation> : EtlTransformer<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation, JsHandleJint>
        where TExtracted : ExtractedItem
        where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
        where TEtlPerformanceOperation : EtlPerformanceOperation
    {
        protected EtlTransformerJint(DocumentDatabase database, DocumentsOperationContext context, PatchRequest mainScript, PatchRequest behaviorFunctions)
            : base(database, context, mainScript, behaviorFunctions)
        {
            if (_behaviorFunctions != null)
                _behaviorFunctionsRun = Database.Scripts.GetScriptRunnerJint(_behaviorFunctions, readOnly: true, out BehaviorsScript, executeScriptsSource: false);

            _returnMainRun = Database.Scripts.GetScriptRunnerJint(_mainScript, readOnly: true, out DocumentScript, executeScriptsSource: false);
        }

        protected override ScriptRunnerResult<JsHandleJint> CreateScriptRunnerResult(object obj)
        {
            return new ScriptRunnerResultJint(DocumentScript, DocumentEngineHandle.FromObjectGen(obj, keepAlive: false)); //TODO: egor true/false?
        }
    }

    public abstract class EtlTransformerV8<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation> : EtlTransformer<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation, JsHandleV8>
        where TExtracted : ExtractedItem
        where TStatsScope : AbstractEtlStatsScope<TStatsScope, TEtlPerformanceOperation>
        where TEtlPerformanceOperation : EtlPerformanceOperation
    {
        protected EtlTransformerV8(DocumentDatabase database, DocumentsOperationContext context, PatchRequest mainScript, PatchRequest behaviorFunctions)
            : base(database, context, mainScript, behaviorFunctions)
        {
            if (_behaviorFunctions != null)
                _behaviorFunctionsRun = Database.Scripts.GetScriptRunnerV8(_behaviorFunctions, readOnly: true, out BehaviorsScript, executeScriptsSource: false);

            _returnMainRun = Database.Scripts.GetScriptRunnerV8(_mainScript, readOnly: true, out DocumentScript, executeScriptsSource: false);
        }

        protected override ScriptRunnerResult<JsHandleV8> CreateScriptRunnerResult(object obj)
        {
            return new ScriptRunnerResultV8(DocumentScript, DocumentEngineHandle.FromObjectGen(obj, keepAlive:false)); //TODO: egor true/false?
        }
    }
}
