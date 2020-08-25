using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Runtime.Interop;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlTransformer<TExtracted, TTransformed> : IDisposable where TExtracted : ExtractedItem
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

            for (var i = 0; i < LoadToDestinations.Length; i++)
            {
                var collection = LoadToDestinations[i];
                var name = Transformation.LoadTo + collection;
                var clrFunctionInstance = new ClrFunctionInstance(DocumentScript.ScriptEngine, name, (value, values) => LoadToFunctionTranslator(collection, value, values));
                DocumentScript.ScriptEngine.SetValue(name, clrFunctionInstance);
            }

            DocumentScript.ScriptEngine.SetValue(Transformation.LoadAttachment, new ClrFunctionInstance(DocumentScript.ScriptEngine, Transformation.LoadAttachment, LoadAttachment));

            DocumentScript.ScriptEngine.SetValue(Transformation.LoadCounter, new ClrFunctionInstance(DocumentScript.ScriptEngine, Transformation.LoadCounter, LoadCounter));

            DocumentScript.ScriptEngine.SetValue("getAttachments", new ClrFunctionInstance(DocumentScript.ScriptEngine, "getAttachments", GetAttachments));

            DocumentScript.ScriptEngine.SetValue("hasAttachment", new ClrFunctionInstance(DocumentScript.ScriptEngine, "hasAttachment", HasAttachment));

            DocumentScript.ScriptEngine.SetValue("getCounters", new ClrFunctionInstance(DocumentScript.ScriptEngine, "getCounters", GetCounters));

            DocumentScript.ScriptEngine.SetValue("hasCounter", new ClrFunctionInstance(DocumentScript.ScriptEngine, "hasCounter", HasCounter));
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

        private JsValue LoadAttachment(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidScriptMethodCall($"{Transformation.LoadAttachment}(name) must have a single string argument");

            var attachmentName = args[0].AsString();
            JsValue loadAttachmentReference = (JsValue)Transformation.AttachmentMarker + attachmentName;

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

        private JsValue LoadCounter(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidScriptMethodCall($"{Transformation.LoadCounter}(name) must have a single string argument");

            var counterName = args[0].AsString();
            JsValue loadCounterReference = (JsValue)Transformation.CounterMarker + counterName;

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

        protected abstract string[] LoadToDestinations { get; }

        protected abstract void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject);

        public abstract List<TTransformed> GetTransformedResults();

        public abstract void Transform(TExtracted item, EtlStatsScope stats);

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
