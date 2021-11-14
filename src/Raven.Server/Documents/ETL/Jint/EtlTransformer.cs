using System;
using System.Collections.Generic;
using System.Linq;
using Jint;
using Jint.Native;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.Indexes.Static.JavaScript.Jint;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.TimeSeries;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL
{
    public abstract partial class EtlTransformer<TExtracted, TTransformed, TStatsScope, TEtlPerformanceOperation>
    {
        protected JintEngineEx DocumentEngineJintEx;
        
        public virtual void InitializeJint()
        {
            DocumentEngineJintEx = _jsOptions.EngineType == JavaScriptEngineType.Jint ? (JintEngineEx)DocumentEngineHandle : null;
        }

        private JsValue StubJint(JsValue self, JsValue[] args)
        {
            return DynamicJsNullJint.ImplicitNullJint;
        }

        private JsValue LoadToFunctionTranslatorJint(JsValue self, JsValue[] args)
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
            var result = new ScriptRunnerResult(DocumentScript, new JsHandle(args[1].AsObject()));
            LoadToFunction(args[0].AsString(), result);
            return result.Instance.Jint.Item;
        }

        private JsValue LoadToFunctionTranslatorInnerJint(string name, JsValue self, JsValue[] args)
        {
            if (args.Length != 1)
                ThrowInvalidScriptMethodCall($"loadTo{name}(obj) must be called with exactly 1 parameter");

            if (args[0].IsObject() == false)
                ThrowInvalidScriptMethodCall($"loadTo{name}(obj) argument must be an object");

            // explicitly not disposing here, this will clear the context from the JavaScriptUtils, but this is 
            // called _midway_ through the script, so that is not something that we want to do. The caller will
            // already be calling that.
            var result = new ScriptRunnerResult(DocumentScript, new JsHandle(args[0].AsObject()));
            LoadToFunction(name, result);
            return result.Instance.Jint.Item;
        }

        protected abstract void AddLoadedAttachmentJint(JsValue reference, string name, Attachment attachment);

        protected abstract void AddLoadedCounterJint(JsValue reference, string name, long value);
        
        protected abstract void AddLoadedTimeSeriesJint(JsValue reference, string name, IEnumerable<SingleResult> entries);

        private JsValue LoadAttachmentJint(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidScriptMethodCall($"{Transformation.LoadAttachment}(name) must have a single string argument");

            var attachmentName = args[0].AsString();
            var loadAttachmentReference = CreateLoadAttachmentReferenceJint(attachmentName);

            if ((Current.Document.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
            {
                var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(Context, Current.DocumentId, attachmentName, AttachmentType.Document, null);

                if (attachment == null)
                    return JsValue.Null;

                AddLoadedAttachmentJint(loadAttachmentReference, attachmentName, attachment);
            }
            else
            {
                return JsValue.Null;
            }

            return loadAttachmentReference;
        }

        private static JsValue CreateLoadAttachmentReferenceJint(string attachmentName)
        {
            return $"{Transformation.AttachmentMarker}{attachmentName}{Guid.NewGuid():N}";
        }

        private JsValue LoadCounterJint(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidScriptMethodCall($"{Transformation.CountersTransformation.Load}(name) must have a single string argument");

            var counterName = args[0].AsString();
            var loadCounterReference = CreateLoadCounterReferenceJint(counterName);

            if ((Current.Document.Flags & DocumentFlags.HasCounters) == DocumentFlags.HasCounters)
            {
                var value = Database.DocumentsStorage.CountersStorage.GetCounterValue(Context, Current.DocumentId, counterName);

                if (value == null)
                    return JsValue.Null;

                AddLoadedCounterJint(loadCounterReference, counterName, value.Value.Value);
            }
            else
            {
                return JsValue.Null;
            }

            return loadCounterReference;
        }

        private static JsValue CreateLoadCounterReferenceJint(string counterName)
        {
            return Transformation.CountersTransformation.Marker + counterName;
        }

        private JsValue LoadTimeSeriesJint(JsValue self, JsValue[] args)
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
                
            var loadTimeSeriesReference = CreateLoadTimeSeriesReferenceJint(timeSeriesName, @from, to);

            var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(Context, Current.DocumentId, timeSeriesName, from, to);
            if(reader.AllValues().Any() == false)
                return JsValue.Null;

            AddLoadedTimeSeriesJint(loadTimeSeriesReference, timeSeriesName, reader.AllValues());

            return loadTimeSeriesReference;
        }

        private static JsValue CreateLoadTimeSeriesReferenceJint(string timeSeriesName, DateTime from, DateTime to)
        {
            return Transformation.TimeSeriesTransformation.Marker + timeSeriesName + from.Ticks + ':' + to.Ticks;
        }

        private JsValue GetAttachmentsJint(JsValue self, JsValue[] args)
        {
            var engine = DocumentEngineJintEx;
            if (args.Length != 0)
                ThrowInvalidScriptMethodCall("getAttachments() must be called without any argument");

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachmentsBlittableArray) == false)
            {
                return engine.Realm.Intrinsics.Array.Construct(Array.Empty<JsValue>());
            }

            var attachments = new JsValue[attachmentsBlittableArray.Length];

            for (int i = 0; i < attachmentsBlittableArray.Length; i++)
            {
                attachments[i] = DocumentScript.TranslateToJs(Context, attachmentsBlittableArray[i]).Jint.Item;
            }

            return engine.Realm.Intrinsics.Array.Construct(attachments);
        }

        private JsValue HasAttachmentJint(JsValue self, JsValue[] args)
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

        private JsValue GetCountersJint(JsValue self, JsValue[] args)
        {
            var engine = DocumentEngineJintEx;
            if (args.Length != 0)
                ThrowInvalidScriptMethodCall("getCounters() must be called without any argument");

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray countersArray) == false)
            {
                return engine.Realm.Intrinsics.Array.Construct(Array.Empty<JsValue>());
            }

            var counters = new JsValue[countersArray.Length];

            for (int i = 0; i < countersArray.Length; i++)
            {
                counters[i] = DocumentScript.TranslateToJs(Context, countersArray[i]).Jint.Item;
            }

            return engine.Realm.Intrinsics.Array.Construct(counters);
        }

        private JsValue HasCounterJint(JsValue self, JsValue[] args)
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

        private JsValue GetTimeSeriesJint(JsValue self, JsValue[] args)
        {
            const int paramsCount = Transformation.TimeSeriesTransformation.GetTimeSeries.ParamsCount;
            const string signature = Transformation.TimeSeriesTransformation.GetTimeSeries.Signature;
            
            var engine = DocumentEngineJintEx;
            if (args.Length != paramsCount)
                ThrowInvalidScriptMethodCall($"{signature} must be called without any argument");

            if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) != DocumentFlags.HasTimeSeries)
                return false;
            
            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeriesArray) == false)
            {
                return engine.Realm.Intrinsics.Array.Construct(Array.Empty<JsValue>());
            }

            var timeSeriesNames = new JsValue[timeSeriesArray.Length];
            for (int i = 0; i < timeSeriesArray.Length; i++)
            {
                timeSeriesNames[i] = DocumentScript.TranslateToJs(Context, timeSeriesArray[i]).Jint.Item;
            }
            return engine.Realm.Intrinsics.Array.Construct(timeSeriesNames);
        }

        private JsValue HasTimeSeriesJint(JsValue self, JsValue[] args)
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
    }
}
