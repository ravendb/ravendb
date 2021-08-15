using System;
using System.Collections.Generic;
using System.Linq;
using V8.Net;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Extensions;
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

        public V8EngineEx Engine;

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

            Engine = DocumentScript.ScriptEngine;
            Engine.GlobalObject.SetProperty(Transformation.LoadTo, Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(LoadToFunctionTranslator));

            foreach (var collection in LoadToDestinations)
            {
                var name = Transformation.LoadTo + collection;
                Engine.GlobalObject.SetProperty(name, Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(LoadToFunctionTranslator));
            }

            Engine.GlobalObject.SetProperty(Transformation.LoadAttachment, Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(LoadAttachment));

            const string loadCounter = Transformation.CountersTransformation.Load;
            Engine.GlobalObject.SetProperty(loadCounter, Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(LoadCounter));

            const string loadTimeSeries = Transformation.TimeSeriesTransformation.LoadTimeSeries.Name;
            Engine.GlobalObject.SetProperty(loadTimeSeries, Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(LoadTimeSeries));

            Engine.GlobalObject.SetProperty("getAttachments", Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(GetAttachments));

            Engine.GlobalObject.SetProperty("hasAttransfochment", Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(HasAttachment));

            Engine.GlobalObject.SetProperty("getCounters", Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(GetCounters));

            Engine.GlobalObject.SetProperty("hasCounter", Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(HasCounter));
            
            const string hasTimeSeries = Transformation.TimeSeriesTransformation.HasTimeSeries.Name;
            Engine.GlobalObject.SetProperty(hasTimeSeries, Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(HasTimeSeries));
            
            const string getTimeSeries = Transformation.TimeSeriesTransformation.GetTimeSeries.Name;
            Engine.GlobalObject.SetProperty(getTimeSeries, Engine.CreateFunctionTemplate().GetFunctionObject<V8Function>(GetTimeSeries));
        }

        private InternalHandle LoadToFunctionTranslator(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                InternalHandle jsRes = InternalHandle.Empty;
                if (args.Length != 2)
                    ThrowInvalidScriptMethodCall("loadTo(name, obj) must be called with exactly 2 parameters");
                if (args[0].IsStringEx() == false)
                    ThrowInvalidScriptMethodCall("loadTo(name, obj) first argument must be a string");
                if (args[1].IsObject == false)
                    ThrowInvalidScriptMethodCall("loadTo(name, obj) second argument must be an object");

                // explicitly not disposing here, this will clear the context from the JavaScriptUtils, but this is 
                // called _midway_ through the script, so that is not something that we want to do. The caller will
                // already be calling that.
                var result = new ScriptRunnerResult(DocumentScript, args[1]);
                LoadToFunction(args[0].AsString, result);
                return jsRes.Set(result.Instance);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle LoadToFunctionTranslator(string name, InternalHandle self, params InternalHandle[] args)
        {
            InternalHandle jsRes = InternalHandle.Empty;
            if (args.Length != 1)
                ThrowInvalidScriptMethodCall($"loadTo{name}(obj) must be called with exactly 1 parameter");

            if (args[0].IsObject == false)
                ThrowInvalidScriptMethodCall($"loadTo{name}(obj) argument must be an object");

            // explicitly not disposing here, this will clear the context from the JavaScriptUtils, but this is 
            // called _midway_ through the script, so that is not something that we want to do. The caller will
            // already be calling that.
            var result = new ScriptRunnerResult(DocumentScript, args[0]);
            LoadToFunction(name, result);
            return jsRes.Set(result.Instance);
        }

        protected abstract void AddLoadedAttachment(InternalHandle reference, string name, Attachment attachment);

        protected abstract void AddLoadedCounter(InternalHandle reference, string name, long value);
        
        protected abstract void AddLoadedTimeSeries(InternalHandle reference, string name, IEnumerable<SingleResult> entries);

        private InternalHandle LoadAttachment(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try {
                if (args.Length != 1 || args[0].IsStringEx() == false)
                    ThrowInvalidScriptMethodCall($"{Transformation.LoadAttachment}(name) must have a single string argument");

                var attachmentName = args[0].AsString;
                var loadAttachmentReference = CreateLoadAttachmentReference(engine, attachmentName);

                if ((Current.Document.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
                {
                    var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(Context, Current.DocumentId, attachmentName, AttachmentType.Document, null);

                    if (attachment == null) {
                        loadAttachmentReference.Dispose();
                        return engine.CreateNullValue();
                    }

                    AddLoadedAttachment(loadAttachmentReference, attachmentName, attachment);
                }
                else
                {
                    loadAttachmentReference.Dispose();
                    return engine.CreateNullValue();
                }

                return loadAttachmentReference;
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private static InternalHandle CreateLoadAttachmentReference(V8Engine engine, string attachmentName)
        {
            return engine.CreateValue($"{Transformation.AttachmentMarker}{attachmentName}{Guid.NewGuid():N}");
        }

        private InternalHandle LoadCounter(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try {
                if (args.Length != 1 || args[0].IsStringEx() == false)
                    ThrowInvalidScriptMethodCall($"{Transformation.CountersTransformation.Load}(name) must have a single string argument");

                var counterName = args[0].AsString;
                var loadCounterReference = CreateLoadCounterReference(engine, counterName);

                if ((Current.Document.Flags & DocumentFlags.HasCounters) == DocumentFlags.HasCounters)
                {
                    var value = Database.DocumentsStorage.CountersStorage.GetCounterValue(Context, Current.DocumentId, counterName);

                    if (value == null) {
                        loadCounterReference.Dispose();
                        return engine.CreateNullValue();
                    }

                    AddLoadedCounter(loadCounterReference, counterName, value.Value.Value);
                }
                else
                {
                    loadCounterReference.Dispose();
                    return engine.CreateNullValue();
                }

                return loadCounterReference;
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private static InternalHandle CreateLoadCounterReference(V8Engine engine, string counterName)
        {
            return engine.CreateValue(Transformation.CountersTransformation.Marker + counterName);
        }

        private InternalHandle LoadTimeSeries(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try {
                if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) == DocumentFlags.HasTimeSeries == false)
                    return engine.CreateNullValue();

                const int minParamsCount = Transformation.TimeSeriesTransformation.LoadTimeSeries.MinParamsCount;
                const int maxParamsCount = Transformation.TimeSeriesTransformation.LoadTimeSeries.MaxParamsCount;
                const string signature = Transformation.TimeSeriesTransformation.LoadTimeSeries.Signature;
                
                if (args.Length < minParamsCount || args.Length > maxParamsCount)
                    ThrowInvalidScriptMethodCall($"{signature} must have between {minParamsCount} to {maxParamsCount} arguments");
                    
                if(args[0].IsStringEx() == false)
                    ThrowInvalidScriptMethodCall($"{signature}. The argument timeSeriesName must be a string");
                var timeSeriesName = args[0].AsString;

                var from = args.Length < 2 ? DateTime.MinValue : ScriptRunner.GetDateArg(args[1], signature, "from"); 
                var to = args.Length < 3 ? DateTime.MaxValue : ScriptRunner.GetDateArg(args[2], signature, "to"); 
                    
                var loadTimeSeriesReference = CreateLoadTimeSeriesReference(engine, timeSeriesName, @from, to);

                var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(Context, Current.DocumentId, timeSeriesName, from, to);
                if(reader.AllValues().Any() == false) {
                    loadTimeSeriesReference.Dispose();
                    return engine.CreateNullValue();
                }

                AddLoadedTimeSeries(loadTimeSeriesReference, timeSeriesName, reader.AllValues());

                return loadTimeSeriesReference;
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private static InternalHandle CreateLoadTimeSeriesReference(V8Engine engine, string timeSeriesName, DateTime from, DateTime to)
        {
            return engine.CreateValue(Transformation.TimeSeriesTransformation.Marker + timeSeriesName + from.Ticks + ':' + to.Ticks);
        }

        private InternalHandle GetAttachments(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try {
                if (args.Length != 0)
                    ThrowInvalidScriptMethodCall("getAttachments() must be called without any argument");

                if (Current.Document.TryGetMetadata(out var metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachmentsBlittableArray) == false)
                {
                    return Engine.CreateArray(Array.Empty<InternalHandle>());
                }

                int arrayLength = attachmentsBlittableArray.Length;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = (InternalHandle)DocumentScript.Translate(Context, attachmentsBlittableArray[i]);
                }

                return Engine.CreateArrayWithDisposal(jsItems);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle HasAttachment(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try {
                if (args.Length != 1 || args[0].IsStringEx() == false)
                    ThrowInvalidScriptMethodCall("hasAttachment(name) must be called with one argument (string)");

                if ((Current.Document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments)
                    return Engine.CreateValue(false);

                if (Current.Document.TryGetMetadata(out var metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                {
                    return Engine.CreateValue(false);
                }

                var checkedName = args[0].AsString;

                foreach (var attachment in attachments)
                {
                    var attachmentInfo = (BlittableJsonReaderObject)attachment;
                    
                    if (attachmentInfo.TryGet(nameof(AttachmentName.Name), out string name) && checkedName.Equals(name, StringComparison.OrdinalIgnoreCase))
                    {
                        return Engine.CreateValue(true);
                    }
                }

                return Engine.CreateValue(false);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle GetCounters(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try {
                if (args.Length != 0)
                    ThrowInvalidScriptMethodCall("getCounters() must be called without any argument");

                if (Current.Document.TryGetMetadata(out var metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray countersArray) == false)
                {
                    return Engine.CreateArray(Array.Empty<InternalHandle>());
                }

                int arrayLength = countersArray.Length;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = (InternalHandle)DocumentScript.Translate(Context, countersArray[i]);
                }

                return Engine.CreateArrayWithDisposal(jsItems);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle HasCounter(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try {
                if (args.Length != 1 || args[0].IsStringEx() == false)
                    ThrowInvalidScriptMethodCall("hasCounter(name) must be called with one argument (string)");

                if ((Current.Document.Flags & DocumentFlags.HasCounters) != DocumentFlags.HasCounters)
                    return engine.CreateValue(false);

                if (Current.Document.TryGetMetadata(out var metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters) == false)
                {
                    return engine.CreateValue(false);
                }

                var checkedName = args[0].AsString;

                foreach (var counter in counters)
                {
                    var counterName = (LazyStringValue)counter;

                    if (checkedName.Equals(counterName, StringComparison.OrdinalIgnoreCase))
                        return engine.CreateValue(true);
                }

                return engine.CreateValue(false);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle GetTimeSeries(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try {
                const int paramsCount = Transformation.TimeSeriesTransformation.GetTimeSeries.ParamsCount;
                const string signature = Transformation.TimeSeriesTransformation.GetTimeSeries.Signature;
                
                if (args.Length != paramsCount)
                    ThrowInvalidScriptMethodCall($"{signature} must be called without any argument");

                if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) != DocumentFlags.HasTimeSeries)
                    return engine.CreateValue(false);
                
                if (Current.Document.TryGetMetadata(out var metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeriesArray) == false)
                {
                    return Engine.CreateArray(Array.Empty<InternalHandle>());
                }

                var jsItems = new InternalHandle[timeSeriesArray.Length];
                for (int i = 0; i < timeSeriesArray.Length; i++)
                {
                    jsItems[i] = (InternalHandle)DocumentScript.Translate(Context, timeSeriesArray[i]);
                }
                return Engine.CreateArrayWithDisposal(jsItems);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle HasTimeSeries(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try {
                const int paramsCount = Transformation.TimeSeriesTransformation.HasTimeSeries.ParamsCount;
                const string signature = Transformation.TimeSeriesTransformation.HasTimeSeries.Signature;

                if (args.Length != paramsCount || args[0].IsStringEx() == false)
                    ThrowInvalidScriptMethodCall($"{signature} must be called with one argument (string)");

                if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) != DocumentFlags.HasTimeSeries)
                    return engine.CreateValue(false);

                if (Current.Document.TryGetMetadata(out var metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeriesNames) == false)
                {
                    return engine.CreateValue(false);
                }

                var checkedName = args[0].AsString;

                foreach (var timeSeries in timeSeriesNames)
                {
                    var counterName = (LazyStringValue)timeSeries;
                    if (checkedName.Equals(counterName, StringComparison.OrdinalIgnoreCase))
                        return engine.CreateValue(true);
                }

                return engine.CreateValue(false);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
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
