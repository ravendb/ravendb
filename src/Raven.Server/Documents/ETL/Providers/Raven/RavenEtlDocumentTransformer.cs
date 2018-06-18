using System;
using System.Collections.Generic;
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
            : base(database, context, script.Transformation)
        {
            _transformation = transformation;
            _script = script;

            LoadToDestinations = _script.Transformation == null ? new string[0] : _script.LoadToCollections;
        }

        public override void Initalize()
        {
            base.Initalize();

            if (SingleRun == null)
                return;

            if (_transformation.IsHandlingAttachments)
                _addAttachmentMethod = new PropertyDescriptor(new ClrFunctionInstance(SingleRun.ScriptEngine, AddAttachment), null, null, null);

            if (_transformation.IsHandlingCounters)
                _addCounterMethod = new PropertyDescriptor(new ClrFunctionInstance(SingleRun.ScriptEngine, AddCounter), null, null, null);
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

            if (_transformation.IsHandlingAttachments)
            {
                var docInstance = (ObjectInstance)document.Instance;

                docInstance.DefineOwnProperty(Transformation.AddAttachment, _addAttachmentMethod, throwOnError: true);
            }

            if (_transformation.IsHandlingCounters)
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
                        ThrowInvalidSriptMethodCall($"First argument of {Transformation.AddAttachment}(name, attachment) must be string");

                    name = args[0].AsString();
                    attachmentReference = args[1];
                    break;
                case 1:
                    attachmentReference = args[0];
                    break;
                default:
                    ThrowInvalidSriptMethodCall($"{Transformation.AddAttachment} must have one or two arguments");
                    break;
            }

            if (attachmentReference.IsString() == false || attachmentReference.AsString().StartsWith(Transformation.AttachmentMarker) == false)
            {
                var message =
                    $"{Transformation.AddAttachment}() method expects to get the reference to an attachment while it got argument of '{attachmentReference.Type}' type";

                if (attachmentReference.IsString())
                    message += $" (value: '{attachmentReference.AsString()}')";

                ThrowInvalidSriptMethodCall(message);
            }

            _currentRun.AddAttachment(self, name, attachmentReference);

            return self;
        }

        private JsValue AddCounter(JsValue self, JsValue[] args)
        {
            JsValue counterReference = null;
            string name = null; // will preserve original name

            switch (args.Length)
            {
                case 2:
                    if (args[0].IsString() == false)
                        ThrowInvalidSriptMethodCall($"First argument of {Transformation.AddCounter}(name, counter) must be string");

                    name = args[0].AsString();
                    counterReference = args[1];
                    break;
                case 1:
                    counterReference = args[0];
                    break;
                default:
                    ThrowInvalidSriptMethodCall($"{Transformation.AddCounter} must have one or two arguments");
                    break;
            }

            if (counterReference.IsString() == false || counterReference.AsString().StartsWith(Transformation.CounterMarker) == false)
            {
                var message =
                    $"{Transformation.AddCounter}() method expects to get the reference to an attachment while it got argument of '{counterReference.Type}' type";

                if (counterReference.IsString())
                    message += $" (value: '{counterReference.AsString()}')";

                ThrowInvalidSriptMethodCall(message);
            }

            _currentRun.AddCounter(self, name, counterReference);

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
                        if (_script.Transformation != null)
                        {
                            if (_script.LoadToCollections.Length > 1 || _script.IsLoadedToDefaultCollection(item, _script.LoadToCollections[0]) == false)
                            {
                                // first, we need to delete docs prefixed by modified document ID to properly handle updates of 
                                // documents loaded to non default collections

                                ApplyDeleteCommands(item, OperationType.Put);
                            }

                            SingleRun.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();
                        }
                        else
                        {
                            _currentRun.PutDocumentAndAttachments(item.DocumentId, item.Document.Data, GetAttachmentsFor(item));
                        }

                        break;
                    case EtlItemType.Counter:
                        if (_script.Transformation != null)
                        {
                            throw new NotImplementedException("TODO arek");
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
                        if (_script.Transformation != null)
                            ApplyDeleteCommands(item, OperationType.Delete);
                        else
                            _currentRun.Delete(new DeleteCommandData(item.DocumentId, null));
                        break;
                    case EtlItemType.Counter:
                        throw  new NotImplementedException("TODO arek");
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

        private void ApplyDeleteCommands(RavenEtlItem item, OperationType operation)
        {
            for (var i = 0; i < _script.LoadToCollections.Length; i++)
            {
                var collection = _script.LoadToCollections[i];

                if (_script.IsLoadedToDefaultCollection(item, collection))
                {
                    if (operation == OperationType.Delete || _transformation.IsHandlingAttachments || _transformation.IsHandlingCounters)
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

            public readonly HashSet<string> DefaultCollections;

            public readonly Dictionary<string, string> IdPrefixForCollection = new Dictionary<string, string>();

            public ScriptInput(Transformation transformation)
            {
                DefaultCollections = new HashSet<string>(transformation.Collections, StringComparer.OrdinalIgnoreCase);

                if (string.IsNullOrEmpty(transformation.Script))
                    return;

                Transformation = new PatchRequest(transformation.Script, PatchRequestType.RavenEtl);

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
