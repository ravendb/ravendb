using System;
using System.Collections.Generic;
using System.Linq;
using V8.Net;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Documents.TimeSeries;
using Sparrow.Json;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;

namespace Raven.Server.Documents.ETL
{
    public abstract partial class EtlTransformer<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation>
    {
        protected V8EngineEx DocumentEngineV8Ex;
        protected V8Engine DocumentEngineV8;
        
        public virtual void InitializeV8()
        {
            DocumentEngineV8Ex = _jsOptions.EngineType == JavaScriptEngineType.V8 ? (V8EngineEx)DocumentEngineHandle : null;
            DocumentEngineV8 = DocumentEngineV8Ex;
        }

        private InternalHandle StubV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            return self.Clone(); // [shlomo] another option to call properties on it is: DocumentEngineV8Ex.ImplicitNullV8;
        }

        private InternalHandle LoadToFunctionTranslatorV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try
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
                var result = new ScriptRunnerResult(DocumentScript, new JsHandle(args[1]));
                LoadToFunction(args[0].AsString, result);
                return new InternalHandle(ref result.Instance.V8.Item, true);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle LoadToFunctionTranslatorInnerV8(string name, InternalHandle self, params InternalHandle[] args)
        {
            if (args.Length != 1)
                ThrowInvalidScriptMethodCall($"loadTo{name}(obj) must be called with exactly 1 parameter");

            if (args[0].IsObject == false)
                ThrowInvalidScriptMethodCall($"loadTo{name}(obj) argument must be an object");

            // explicitly not disposing here, this will clear the context from the JavaScriptUtils, but this is 
            // called _midway_ through the script, so that is not something that we want to do. The caller will
            // already be calling that.
            var result = new ScriptRunnerResult(DocumentScript, new JsHandle(args[0]));
            LoadToFunction(name, result);
            return result.Instance.V8.Item.Clone();
        }

        protected abstract void AddLoadedAttachmentV8(InternalHandle reference, string name, Attachment attachment);

        protected abstract void AddLoadedCounterV8(InternalHandle reference, string name, long value);
        
        protected abstract void AddLoadedTimeSeriesV8(InternalHandle reference, string name, IEnumerable<SingleResult> entries);

        private InternalHandle LoadAttachmentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try
            {
                if (args.Length != 1 || args[0].IsStringEx == false)
                    ThrowInvalidScriptMethodCall($"{Transformation.LoadAttachment}(name) must have a single string argument");

                var attachmentName = args[0].AsString;
                var loadAttachmentReference = CreateLoadAttachmentReferenceV8(engine, attachmentName);

                if ((Current.Document.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
                {
                    var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(Context, Current.DocumentId, attachmentName, AttachmentType.Document, null);

                    if (attachment == null)
                    {
                        loadAttachmentReference.Dispose();
                        return engine.CreateNullValue();
                    }

                    AddLoadedAttachmentV8(loadAttachmentReference, attachmentName, attachment);
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
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private static InternalHandle CreateLoadAttachmentReferenceV8(V8Engine engine, string attachmentName)
        {
            return engine.CreateValue($"{Transformation.AttachmentMarker}{attachmentName}{Guid.NewGuid():N}");
        }

        private InternalHandle LoadCounterV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try
            {
                if (args.Length != 1 || args[0].IsStringEx == false)
                    ThrowInvalidScriptMethodCall($"{Transformation.CountersTransformation.Load}(name) must have a single string argument");

                var counterName = args[0].AsString;
                var loadCounterReference = CreateLoadCounterReferenceV8(engine, counterName);

                if ((Current.Document.Flags & DocumentFlags.HasCounters) == DocumentFlags.HasCounters)
                {
                    var value = Database.DocumentsStorage.CountersStorage.GetCounterValue(Context, Current.DocumentId, counterName);

                    if (value == null)
                    {
                        loadCounterReference.Dispose();
                        return engine.CreateNullValue();
                    }

                    AddLoadedCounterV8(loadCounterReference, counterName, value.Value.Value);
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
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private static InternalHandle CreateLoadCounterReferenceV8(V8Engine engine, string counterName)
        {
            return engine.CreateValue(Transformation.CountersTransformation.Marker + counterName);
        }

        private InternalHandle LoadTimeSeriesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try
            {
                if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) == DocumentFlags.HasTimeSeries == false)
                    return engine.CreateNullValue();

                const int minParamsCount = Transformation.TimeSeriesTransformation.LoadTimeSeries.MinParamsCount;
                const int maxParamsCount = Transformation.TimeSeriesTransformation.LoadTimeSeries.MaxParamsCount;
                const string signature = Transformation.TimeSeriesTransformation.LoadTimeSeries.Signature;
                
                if (args.Length < minParamsCount || args.Length > maxParamsCount)
                    ThrowInvalidScriptMethodCall($"{signature} must have between {minParamsCount} to {maxParamsCount} arguments");
                    
                if(args[0].IsStringEx == false)
                    ThrowInvalidScriptMethodCall($"{signature}. The argument timeSeriesName must be a string");
                var timeSeriesName = args[0].AsString;

                var from = args.Length < 2 ? DateTime.MinValue : ScriptRunner.GetDateArg(args[1], signature, "from"); 
                var to = args.Length < 3 ? DateTime.MaxValue : ScriptRunner.GetDateArg(args[2], signature, "to"); 
                    
                var loadTimeSeriesReference = CreateLoadTimeSeriesReferenceV8(engine, timeSeriesName, @from, to);

                var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(Context, Current.DocumentId, timeSeriesName, from, to);
                if(reader.AllValues().Any() == false)
                {
                    loadTimeSeriesReference.Dispose();
                    return engine.CreateNullValue();
                }

                AddLoadedTimeSeriesV8(loadTimeSeriesReference, timeSeriesName, reader.AllValues());

                return loadTimeSeriesReference;
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private static InternalHandle CreateLoadTimeSeriesReferenceV8(V8Engine engine, string timeSeriesName, DateTime from, DateTime to)
        {
            return engine.CreateValue(Transformation.TimeSeriesTransformation.Marker + timeSeriesName + from.Ticks + ':' + to.Ticks);
        }

        private InternalHandle GetAttachmentsV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            var engineEx = DocumentEngineV8Ex;
            try
            {
                if (args.Length != 0)
                    ThrowInvalidScriptMethodCall("getAttachments() must be called without any argument");

                if (Current.Document.TryGetMetadata(out var metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachmentsBlittableArray) == false)
                {
                    return engine.CreateArray(Array.Empty<InternalHandle>());
                }

                int arrayLength = attachmentsBlittableArray.Length;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = DocumentScript.TranslateToJs(Context, attachmentsBlittableArray[i], true).V8.Item;
                }

                return engineEx.CreateArrayWithDisposal(jsItems);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle HasAttachmentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try
            {
                if (args.Length != 1 || args[0].IsStringEx == false)
                    ThrowInvalidScriptMethodCall("hasAttachment(name) must be called with one argument (string)");

                if ((Current.Document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments)
                    return engine.CreateValue(false);

                if (Current.Document.TryGetMetadata(out var metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                {
                    return engine.CreateValue(false);
                }

                var checkedName = args[0].AsString;

                foreach (var attachment in attachments)
                {
                    var attachmentInfo = (BlittableJsonReaderObject)attachment;
                    
                    if (attachmentInfo.TryGet(nameof(AttachmentName.Name), out string name) && checkedName.Equals(name, StringComparison.OrdinalIgnoreCase))
                    {
                        return engine.CreateValue(true);
                    }
                }

                return engine.CreateValue(false);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle GetCountersV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            var engineEx = DocumentEngineV8Ex;
            try
            {
                if (args.Length != 0)
                    ThrowInvalidScriptMethodCall("getCountersV8() must be called without any argument");

                if (Current.Document.TryGetMetadata(out var metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray countersArray) == false)
                {
                    return engine.CreateArray(Array.Empty<InternalHandle>());
                }

                int arrayLength = countersArray.Length;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = DocumentScript.TranslateToJs(Context, countersArray[i], true).V8.Item;
                }

                return engineEx.CreateArrayWithDisposal(jsItems);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle HasCounterV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try
            {
                if (args.Length != 1 || args[0].IsStringEx == false)
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
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle GetTimeSeriesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            var engineEx = DocumentEngineV8Ex;
            try
            {
                const int paramsCount = Transformation.TimeSeriesTransformation.GetTimeSeries.ParamsCount;
                const string signature = Transformation.TimeSeriesTransformation.GetTimeSeries.Signature;
                
                if (args.Length != paramsCount)
                    ThrowInvalidScriptMethodCall($"{signature} must be called without any argument");

                if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) != DocumentFlags.HasTimeSeries)
                    return engine.CreateValue(false);
                
                if (Current.Document.TryGetMetadata(out var metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeriesArray) == false)
                {
                    return engine.CreateArray(Array.Empty<InternalHandle>());
                }

                var jsItems = new InternalHandle[timeSeriesArray.Length];
                for (int i = 0; i < timeSeriesArray.Length; i++)
                {
                    jsItems[i] = DocumentScript.TranslateToJs(Context, timeSeriesArray[i], true).V8.Item;
                }
                return engineEx.CreateArrayWithDisposal(jsItems);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        private InternalHandle HasTimeSeriesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
        {
            try
            {
                const int paramsCount = Transformation.TimeSeriesTransformation.HasTimeSeries.ParamsCount;
                const string signature = Transformation.TimeSeriesTransformation.HasTimeSeries.Signature;

                if (args.Length != paramsCount || args[0].IsStringEx == false)
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
                return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }        
    }
}
