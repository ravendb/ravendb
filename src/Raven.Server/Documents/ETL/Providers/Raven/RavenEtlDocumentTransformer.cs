using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

// ReSharper disable ForCanBeConvertedToForeach

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlDocumentTransformer : EtlTransformer<RavenEtlItem, ICommandData>
    {
        private readonly Transformation _transformation;
        private readonly ScriptInput _script;
        private readonly List<ICommandData> _commands = new List<ICommandData>();
        private PropertyDescriptor _addAttachmentMethod;
        private PropertyDescriptor _addCounterMethod;
        private RavenEtlScriptRun _currentRun;

        public RavenEtlDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, ScriptInput script)
            : base(database, context, script.Transformation, script.BehaviorFunctions)
        {
            _transformation = transformation;
            _script = script;

            LoadToDestinations = _script.LoadToCollections;
        }

        public override void Initialize(bool debugMode)
        {
            base.Initialize(debugMode);

            if (DocumentScript == null)
                return;

            if (_transformation.IsAddingAttachments)
                _addAttachmentMethod = new PropertyDescriptor(new ClrFunctionInstance(DocumentScript.ScriptEngine, AddAttachment), null, null, null);

            if (_transformation.IsAddingCounters)
                _addCounterMethod = new PropertyDescriptor(new ClrFunctionInstance(DocumentScript.ScriptEngine, AddCounter), null, null, null);
        }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string collectionName, ScriptRunnerResult document)
        {
            if (collectionName == null)
                ThrowLoadParameterIsMandatory(nameof(collectionName));

            string id;
            var loadedToDifferentCollection = false;

            if (_script.IsLoadedToDefaultCollection(Current, collectionName))
            {
                id = Current.DocumentId;
            }
            else
            {
                id = GetPrefixedId(Current.DocumentId, collectionName);
                loadedToDifferentCollection = true;
            }

            var metadata = document.GetOrCreate(Constants.Documents.Metadata.Key);

            if (loadedToDifferentCollection || metadata.HasProperty(Constants.Documents.Metadata.Collection) == false)
                metadata.Put(Constants.Documents.Metadata.Collection, collectionName, throwOnError: true);

            if (metadata.HasProperty(Constants.Documents.Metadata.Id) == false)
                metadata.Put(Constants.Documents.Metadata.Id, id, throwOnError: true);

            if (metadata.HasProperty(Constants.Documents.Metadata.Attachments))
                metadata.Delete(Constants.Documents.Metadata.Attachments, throwOnError: true);

            if (metadata.HasProperty(Constants.Documents.Metadata.Counters))
                metadata.Delete(Constants.Documents.Metadata.Counters, throwOnError: true);

            var transformed = document.TranslateToObject(Context);

            var transformResult = Context.ReadObject(transformed, id);

            _currentRun.Put(id, document.Instance, transformResult);

            if (_transformation.IsAddingAttachments)
            {
                var docInstance = (ObjectInstance)document.Instance;

                docInstance.DefineOwnProperty(Transformation.AddAttachment, _addAttachmentMethod, throwOnError: true);
            }

            if (_transformation.IsAddingCounters)
            {
                var docInstance = (ObjectInstance)document.Instance;

                docInstance.DefineOwnProperty(Transformation.AddCounter, _addCounterMethod, throwOnError: true);
            }
        }

        private JsValue AddAttachment(JsValue self, JsValue[] args)
        {
            JsValue attachmentReference = null;
            string name = null; // will preserve original name

            switch (args.Length)
            {
                case 2:
                    if (args[0].IsString() == false)
                        ThrowInvalidScriptMethodCall($"First argument of {Transformation.AddAttachment}(name, attachment) must be string");

                    name = args[0].AsString();
                    attachmentReference = args[1];
                    break;
                case 1:
                    attachmentReference = args[0];
                    break;
                default:
                    ThrowInvalidScriptMethodCall($"{Transformation.AddAttachment} must have one or two arguments");
                    break;
            }

            if (attachmentReference.IsNull())
                return self;

            if (attachmentReference.IsString() == false || attachmentReference.AsString().StartsWith(Transformation.AttachmentMarker) == false)
            {
                var message =
                    $"{Transformation.AddAttachment}() method expects to get the reference to an attachment while it got argument of '{attachmentReference.Type}' type";

                if (attachmentReference.IsString())
                    message += $" (value: '{attachmentReference.AsString()}')";

                ThrowInvalidScriptMethodCall(message);
            }

            _currentRun.AddAttachment(self, name, attachmentReference);

            return self;
        }

        private JsValue AddCounter(JsValue self, JsValue[] args)
        {
            if (args.Length != 1)
                ThrowInvalidScriptMethodCall($"{Transformation.AddCounter} must have one arguments");

            var counterReference = args[0];

            if (counterReference.IsNull())
                return self;

            if (counterReference.IsString() == false || counterReference.AsString().StartsWith(Transformation.CounterMarker) == false)
            {
                var message =
                    $"{Transformation.AddCounter}() method expects to get the reference to an attachment while it got argument of '{counterReference.Type}' type";

                if (counterReference.IsString())
                    message += $" (value: '{counterReference.AsString()}')";

                ThrowInvalidScriptMethodCall(message);
            }

            _currentRun.AddCounter(self, counterReference);

            return self;
        }

        private string GetPrefixedId(LazyStringValue documentId, string loadCollectionName)
        {
            return $"{documentId}/{_script.IdPrefixForCollection[loadCollectionName]}/";
        }

        public override List<ICommandData> GetTransformedResults()
        {
            return _commands;
        }

        public override void Transform(RavenEtlItem item)
        {
            Current = item;
            _currentRun = new RavenEtlScriptRun();

            if (item.IsDelete == false)
            {
                switch (item.Type)
                {
                    case EtlItemType.Document:
                        if (_script.HasTransformation)
                        {
                            // first, we need to delete docs prefixed by modified document ID to properly handle updates of 
                            // documents loaded to non default collections

                            ApplyDeleteCommands(item, OperationType.Put);

                            DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();

                            if (_script.HasLoadCounterBehaviors && _script.TryGetLoadCounterBehaviorFunctionFor(item.Collection, out var function))
                            {
                                var counters = GetCountersFor(Current);

                                if (counters != null && counters.Count > 0)
                                {
                                    foreach (var counter in counters)
                                    {
                                        using (var result = BehaviorsScript.Run(Context, Context, function, new object[] { item.DocumentId, counter.Name }))
                                        {
                                            if (result.BooleanValue == true)
                                                _currentRun.AddCounter(item.DocumentId, counter.Name, counter.Value);
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            _currentRun.PutFullDocument(item.DocumentId, item.Document.Data, GetAttachmentsFor(item), GetCountersFor(item));
                        }

                        break;
                    case EtlItemType.Counter:
                        if (_script.HasTransformation)
                        {
                            if (_script.HasLoadCounterBehaviors == false)
                                break;

                            if (_script.TryGetLoadCounterBehaviorFunctionFor(item.Collection, out var function) == false)
                                break;

                            using (var result = BehaviorsScript.Run(Context, Context, function, new object[] {item.DocumentId, item.CounterName}))
                            {
                                if (result.BooleanValue == true)
                                    _currentRun.AddCounter(item.DocumentId, item.CounterName, item.CounterValue);
                            }
                        }
                        else
                        {
                            _currentRun.AddCounter(item.DocumentId, item.CounterName, item.CounterValue);
                        }
                        
                        break;
                }
            }
            else
            {
                switch (item.Type)
                {
                    case EtlItemType.Document:

                        if (ShouldFilterOutDeletion())
                            break;

                        if (_script.HasTransformation)
                        {
                            Debug.Assert(item.IsAttachmentTombstone == false, "attachment tombstones are tracked only if script is empty");

                            ApplyDeleteCommands(item, OperationType.Delete);
                        }
                        else
                        {
                            if (item.IsAttachmentTombstone == false)
                            {
                                _currentRun.Delete(new DeleteCommandData(item.DocumentId, null));
                            }
                            else
                            {
                                var (doc, attachmentName) = AttachmentsStorage.ExtractDocIdAndAttachmentNameFromTombstone(Context, item.AttachmentTombstoneId);

                                _currentRun.DeleteAttachment(doc, attachmentName);
                            }
                        }
                        break;
                    case EtlItemType.Counter:

                        var (docId, counterName) = CountersStorage.ExtractDocIdAndCounterNameFromTombstone(Context, item.CounterTombstoneId);

                        if (_script.HasTransformation)
                        {
                            if (_script.HasLoadCounterBehaviors == false)
                                break;

                            if (_script.TryGetLoadCounterBehaviorFunctionFor(item.Collection, out var function) == false)
                                break;

                            using (var result = BehaviorsScript.Run(Context, Context, function, new object[] { docId, counterName }))
                            {
                                if (result.BooleanValue == true)
                                    _currentRun.DeleteCounter(docId, counterName);
                            }
                        }
                        else
                        {
                            if (ShouldFilterOutDeletion())
                                break;

                            _currentRun.DeleteCounter(docId, counterName);
                        }

                        break;
                }

                bool ShouldFilterOutDeletion()
                {
                    if (_script.HasDeleteDocumentsBehaviors)
                    {
                        var collection = item.Collection ?? item.CollectionFromMetadata;
                        var documentId = item.DocumentId;

                        if (item.IsAttachmentTombstone)
                        {
                            documentId = AttachmentsStorage.ExtractDocIdAndAttachmentNameFromTombstone(Context, item.AttachmentTombstoneId).DocId;

                            Debug.Assert(collection == null);

                            var document = Database.DocumentsStorage.Get(Context, documentId);

                            if (document == null)
                                return true; // document was deleted, no need to send DELETE of attachment tombstone

                            collection = Database.DocumentsStorage.ExtractCollectionName(Context, document.Data).Name;
                        }
                        else if (item.Type == EtlItemType.Counter)
                            documentId = CountersStorage.ExtractDocIdAndCounterNameFromTombstone(Context, item.CounterTombstoneId).DocId;

                        Debug.Assert(collection != null);

                        if (_script.TryGetDeleteDocumentBehaviorFunctionFor(collection, out var function) ||
                            _script.TryGetDeleteDocumentBehaviorFunctionFor(Transformation.GenericDeleteDocumentsBehaviorFunctionKey, out function))
                        {
                            object[] parameters;

                            if (Transformation.GenericDeleteDocumentsBehaviorFunctionName.Equals(function, StringComparison.OrdinalIgnoreCase))
                                parameters = new object[] { documentId, collection };
                            else
                                parameters = new object[] { documentId };

                            using (var result = BehaviorsScript.Run(Context, Context, function, parameters))
                            {
                                if (result.BooleanValue == null || result.BooleanValue == false)
                                    return true;
                            }
                        }
                    }

                    return false;
                }
            }

            _commands.AddRange(_currentRun.GetCommands());
        }

        protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
        {
            _currentRun.LoadAttachment(reference, attachment);
        }

        protected override void AddLoadedCounter(JsValue reference, string name, long value)
        {
            _currentRun.LoadCounter(reference, name, value);
        }

        private List<Attachment> GetAttachmentsFor(RavenEtlItem item)
        {
            if ((Current.Document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments)
                return null;

            if (item.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
            {
                return null;
            }

            if (metadata.Modifications == null)
                metadata.Modifications = new DynamicJsonValue(metadata);

            metadata.Modifications.Remove(Constants.Documents.Metadata.Attachments);

            var results = new List<Attachment>();

            foreach (var attachment in attachments)
            {
                var attachmentInfo = (BlittableJsonReaderObject)attachment;

                if (attachmentInfo.TryGet(nameof(AttachmentName.Name), out string name))
                {
                    var attachmentData = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(Context, item.DocumentId, name, AttachmentType.Document, null);

                    results.Add(attachmentData);
                }
            }

            return results;
        }

        private List<(string Name, long Value)> GetCountersFor(RavenEtlItem item)
        {
            if ((Current.Document.Flags & DocumentFlags.HasCounters) != DocumentFlags.HasCounters)
                return null;

            if (item.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters) == false)
            {
                return null;
            }

            if (metadata.Modifications == null)
                metadata.Modifications = new DynamicJsonValue(metadata);

            metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);

            var results = new List<(string Name, long Value)>();

            foreach (var counter in counters)
            {
                string counterName = (LazyStringValue)counter;

                var value = Database.DocumentsStorage.CountersStorage.GetCounterValue(Context, item.DocumentId, counterName);

                Debug.Assert(value != null);

                results.Add((counterName, value.Value));
            }

            return results;
        }

        private void ApplyDeleteCommands(RavenEtlItem item, OperationType operation)
        {
            for (var i = 0; i < _script.LoadToCollections.Length; i++)
            {
                var collection = _script.LoadToCollections[i];

                if (_script.IsLoadedToDefaultCollection(item, collection))
                {
                    if (operation == OperationType.Delete || _transformation.IsAddingAttachments || _transformation.IsAddingCounters)
                        _currentRun.Delete(new DeleteCommandData(item.DocumentId, null));
                }
                else
                    _currentRun.Delete(new DeletePrefixedCommandData(GetPrefixedId(item.DocumentId, collection)));
            }
        }

        public class ScriptInput
        {
            private readonly Dictionary<string, Dictionary<string, bool>> _collectionNameComparisons;

            public readonly string[] LoadToCollections = new string[0];

            public readonly PatchRequest Transformation;

            public readonly PatchRequest BehaviorFunctions;

            public readonly HashSet<string> DefaultCollections;

            public readonly Dictionary<string, string> IdPrefixForCollection = new Dictionary<string, string>();

            private readonly Dictionary<string, string> _collectionToLoadCounterBehaviorFunction;

            private readonly Dictionary<string, string> _collectionToDeleteDocumentBehaviorFunction;

            public bool HasTransformation => Transformation != null;

            public bool HasLoadCounterBehaviors => _collectionToLoadCounterBehaviorFunction != null;

            public bool HasDeleteDocumentsBehaviors => _collectionToDeleteDocumentBehaviorFunction != null;

            public ScriptInput(Transformation transformation)
            {
                DefaultCollections = new HashSet<string>(transformation.Collections, StringComparer.OrdinalIgnoreCase);

                if (string.IsNullOrWhiteSpace(transformation.Script))
                    return;

                if (transformation.IsEmptyScript == false)
                    Transformation = new PatchRequest(transformation.Script, PatchRequestType.RavenEtl);

                if (transformation.CollectionToLoadCounterBehaviorFunction != null)
                    _collectionToLoadCounterBehaviorFunction = transformation.CollectionToLoadCounterBehaviorFunction;

                if (transformation.CollectionToDeleteDocumentsBehaviorFunction != null)
                    _collectionToDeleteDocumentBehaviorFunction = transformation.CollectionToDeleteDocumentsBehaviorFunction;

                if (HasLoadCounterBehaviors || HasDeleteDocumentsBehaviors)
                    BehaviorFunctions = new PatchRequest(transformation.Script, PatchRequestType.EtlBehaviorFunctions);

                if (transformation.IsEmptyScript == false)
                    LoadToCollections = transformation.GetCollectionsFromScript();

                foreach (var collection in LoadToCollections)
                {
                    IdPrefixForCollection[collection] = DocumentConventions.DefaultTransformCollectionNameToDocumentIdPrefix(collection);
                }

                if (transformation.Collections == null)
                    return;

                _collectionNameComparisons = new Dictionary<string, Dictionary<string, bool>>(transformation.Collections.Count);

                foreach (var sourceCollection in transformation.Collections)
                {
                    _collectionNameComparisons[sourceCollection] = new Dictionary<string, bool>(transformation.Collections.Count);

                    foreach (var loadToCollection in LoadToCollections)
                    {
                        _collectionNameComparisons[sourceCollection][loadToCollection] = string.Compare(sourceCollection, loadToCollection, StringComparison.OrdinalIgnoreCase) == 0;
                    }
                }
            }

            public bool TryGetLoadCounterBehaviorFunctionFor(string collection, out string functionName)
            {
                return _collectionToLoadCounterBehaviorFunction.TryGetValue(collection, out functionName);
            }

            public bool TryGetDeleteDocumentBehaviorFunctionFor(string collection, out string functionName)
            {
                return _collectionToDeleteDocumentBehaviorFunction.TryGetValue(collection, out functionName);
            }

            public bool IsLoadedToDefaultCollection(RavenEtlItem item, string loadToCollection)
            {
                if (item.Collection != null)
                    return _collectionNameComparisons[item.Collection][loadToCollection];

                var collection = item.CollectionFromMetadata;

                return collection?.CompareTo(loadToCollection) == 0;
            }
        }

        private enum OperationType
        {
            Put,
            Delete
        }
    }
}
