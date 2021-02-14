using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
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
        private PropertyDescriptor _addAttachmentMethod;
        private PropertyDescriptor _addCounterMethod;
        private PropertyDescriptor _addTimeSeriesMethod;
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
                _addAttachmentMethod = new PropertyDescriptor(new ClrFunctionInstance(DocumentScript.ScriptEngine, "addAttachment", AddAttachment), null, null, null);

            if (_transformation.Counters.IsAddingCounters)
            {
                const string addCounter = Transformation.CountersTransformation.Add;
                _addCounterMethod = new PropertyDescriptor(new ClrFunctionInstance(DocumentScript.ScriptEngine, addCounter, AddCounter), null, null, null);
            }

            if (_transformation.TimeSeries.IsAddingTimeSeries)
            {
                const string addTimeSeries = Transformation.TimeSeriesTransformation.AddTimeSeries.Name;
                _addTimeSeriesMethod = new PropertyDescriptor(new ClrFunctionInstance(DocumentScript.ScriptEngine, addTimeSeries, AddTimeSeries), null, null, null);
            }
        }

        protected override string[] LoadToDestinations { get; }

        protected override void LoadToFunction(string collectionName, ScriptRunnerResult document)
        {
            if (collectionName == null)
                ThrowLoadParameterIsMandatory(nameof(collectionName));

            string id;
            var loadedToDifferentCollection = false;

            if (_script.MayLoadToDefaultCollection(Current, collectionName))
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
                metadata.Set(Constants.Documents.Metadata.Collection, collectionName, throwOnError: true);

            if (metadata.HasProperty(Constants.Documents.Metadata.Attachments))
                metadata.DeletePropertyOrThrow(Constants.Documents.Metadata.Attachments);

            if (metadata.HasProperty(Constants.Documents.Metadata.Counters))
                metadata.DeletePropertyOrThrow(Constants.Documents.Metadata.Counters);

            var transformed = document.TranslateToObject(Context);

            var transformResult = Context.ReadObject(transformed, id);

            _currentRun.Put(id, document.Instance, transformResult);

            if (_transformation.IsAddingAttachments)
            {
                var docInstance = (ObjectInstance)document.Instance;

                docInstance.DefinePropertyOrThrow(Transformation.AddAttachment, _addAttachmentMethod);
            }

            if (_transformation.Counters.IsAddingCounters)
            {
                var docInstance = (ObjectInstance)document.Instance;

                docInstance.DefinePropertyOrThrow(Transformation.CountersTransformation.Add, _addCounterMethod);
            }
            
            if (_transformation.TimeSeries.IsAddingTimeSeries)
            {
                var docInstance = (ObjectInstance)document.Instance;

                docInstance.DefinePropertyOrThrow(Transformation.TimeSeriesTransformation.AddTimeSeries.Name, _addTimeSeriesMethod);
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
                ThrowInvalidScriptMethodCall($"{Transformation.CountersTransformation.Add} must have one arguments");

            var counterReference = args[0];

            if (counterReference.IsNull())
                return self;

            if (counterReference.IsString() == false || counterReference.AsString().StartsWith(Transformation.CountersTransformation.Marker) == false)
            {
                var message =
                    $"{Transformation.CountersTransformation.Add}() method expects to get the reference to a counter while it got argument of '{counterReference.Type}' type";

                if (counterReference.IsString())
                    message += $" (value: '{counterReference.AsString()}')";

                ThrowInvalidScriptMethodCall(message);
            }

            _currentRun.AddCounter(self, counterReference);

            return self;
        }
        
        private JsValue AddTimeSeries(JsValue self, JsValue[] args)
        {
            if (args.Length != Transformation.TimeSeriesTransformation.AddTimeSeries.ParamsCount)
            {
                ThrowInvalidScriptMethodCall(
                    $"{Transformation.TimeSeriesTransformation.AddTimeSeries.Name} must have {Transformation.TimeSeriesTransformation.AddTimeSeries.ParamsCount} arguments. " +
                    $"Signature `{Transformation.TimeSeriesTransformation.AddTimeSeries.Signature}`");
            }

            var timeSeriesReference = args[0];

            if (timeSeriesReference.IsNull())
                return self;

            if (timeSeriesReference.IsString() == false || timeSeriesReference.AsString().StartsWith(Transformation.TimeSeriesTransformation.Marker) == false)
            {
                var message =
                    $"{Transformation.TimeSeriesTransformation.AddTimeSeries.Name} method expects to get the reference to a time-series while it got argument of '{timeSeriesReference.Type}' type";

                if (timeSeriesReference.IsString())
                    message += $" (value: '{timeSeriesReference.AsString()}')";

                message += $". Signature `{Transformation.TimeSeriesTransformation.AddTimeSeries.Signature}`";
                ThrowInvalidScriptMethodCall(message);
            }

            _currentRun.AddTimeSeries(self, timeSeriesReference);

            return self;
        }

        private string GetPrefixedId(LazyStringValue documentId, string loadCollectionName)
        {
            return $"{documentId}/{_script.IdPrefixForCollection[loadCollectionName]}/";
        }

        public override IEnumerable<ICommandData> GetTransformedResults()
        {
            return _currentRun?.GetCommands() ?? Enumerable.Empty<ICommandData>();
        }

        public override void Transform(RavenEtlItem item, EtlStatsScope stats, EtlProcessState state)
        {
            Current = item;
            _currentRun ??= new RavenEtlScriptRun(stats);

            if (item.IsDelete == false)
            {
                switch (item.Type)
                {
                    case EtlItemType.Document:
                        if (_script.HasTransformation)
                        {
                            DocumentScript.Run(Context, Context, "execute", new object[] { Current.Document }).Dispose();
                            ApplyDeleteCommands(item, OperationType.Put, out var isLoadedToDefaultCollectionDeleted);

                            if (_currentRun.IsDocumentLoadedToSameCollection(item.DocumentId) == false)
                                break;
                            
                            if (_script.TryGetLoadCounterBehaviorFunctionFor(item.Collection, out var counterFunction))
                            {
                                var counterGroups = GetCounterGroupsFor(item);
                                if (counterGroups != null)
                                    AddCounters(item.DocumentId, counterGroups, counterFunction);    
                            }
                            if (_script.TryGetLoadTimeSeriesBehaviorFunctionFor(item.Collection, out var timeSeriesLoadBehaviorFunc))
                            {
                                if (isLoadedToDefaultCollectionDeleted || ShouldLoadTimeSeriesWithDoc(item, state))
                                {
                                    var timeSeriesReaders = GetTimeSeriesFor(item, timeSeriesLoadBehaviorFunc);
                                    if (timeSeriesReaders != null)
                                        AddAndRemoveTimeSeries(item.DocumentId, timeSeriesReaders);
                                }
                            }
                        }
                        else
                        {
                            var attachments = GetAttachmentsFor(item);
                            var counterOperations = GetCounterOperationsFor(item);
                            var timeSeriesOperations = ShouldLoadTimeSeriesWithDoc(item, state) ? GetTimeSeriesOperationsFor(item) : null;
                            _currentRun.PutFullDocument(item.DocumentId, item.Document.Data, attachments, counterOperations, timeSeriesOperations);
                        }
                        break;
                    case EtlItemType.CounterGroup:
                        string cFunction = null;
                        if (_script.HasTransformation)
                        {
                            if (_script.MayLoadToDefaultCollection(item) == false)
                                break;
                            if (_script.TryGetLoadCounterBehaviorFunctionFor(item.Collection, out cFunction) == false)
                                break;
                        }
                        AddSingleCounterGroup(item.DocumentId, item.CounterGroupDocument, cFunction);
                        break;
                    case EtlItemType.TimeSeries:
                        string tsFunction = null;
                        if (_script.HasTransformation)
                        {
                            if (_script.MayLoadToDefaultCollection(item) == false)
                                break;
                            if (_script.TryGetLoadTimeSeriesBehaviorFunctionFor(item.Collection, out tsFunction) == false)
                                break;
                        }
                        HandleSingleTimeSeriesSegment(tsFunction, stats, state);
                        break;
                }
            }
            else
            {
                switch (item.Type)
                {
                    case EtlItemType.Document:
                        if (ShouldFilterOutDeletion(item))
                            break;
                        if (_script.HasTransformation)
                        {
                            Debug.Assert(item.IsAttachmentTombstone == false, "attachment tombstones are tracked only if script is empty");

                            ApplyDeleteCommands(item, OperationType.Delete, out _);
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
                    case EtlItemType.TimeSeries:
                        string function = null;
                        if (_script.HasTransformation)
                        {
                            if (_script.MayLoadToDefaultCollection(item) == false)
                                break;
                            
                            if (_script.TryGetLoadTimeSeriesBehaviorFunctionFor(item.Collection, out function) == false)
                                break;
                        }
                        HandleSingleTimeSeriesDeletedRangeItem(item.TimeSeriesDeletedRangeItem, function);
                        break;
                    default:
                        throw new InvalidOperationException($"Dead Etl item can be of type {EtlItemType.Document} or {EtlItemType.TimeSeries} but got {item.Type}");
                }
            }
        }

        private bool ShouldLoadTimeSeriesWithDoc(RavenEtlItem item, EtlProcessState state)
        {
            //If an Etag of time-series is lower then its document Etag then replication can send the time-series before the document.
            //In this situation Etl process will skip the time-series, mark it as skipped and will send all of it here with the document
            return state.SkippedTimeSeriesDocs != null && state.SkippedTimeSeriesDocs.Remove(item.DocumentId);
        }

        private bool ShouldFilterOutDeletion(RavenEtlItem item)
        {
            if (_script.HasDeleteDocumentsBehaviors)
            {
                var collection = item.Collection ?? item.CollectionFromMetadata;
                var documentId = item.DocumentId;

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


        private void AddCounters(LazyStringValue docId, IEnumerable<CounterGroupDetail> counterGroups, string function = null)
        {
            foreach (var cgd in counterGroups)
            {
                AddSingleCounterGroup(docId, cgd.Values, function);
            }
        }

        private void AddSingleCounterGroup(LazyStringValue docId, BlittableJsonReaderObject counterGroupDocument, string function = null)
        {
            if (counterGroupDocument.TryGet(CountersStorage.Values, out BlittableJsonReaderObject counters) == false)
                return;

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (var i = 0; i < counters.Count; i++)
            {
                counters.GetPropertyByIndex(i, ref prop);

                if (GetCounterValueAndCheckIfShouldSkip(docId, function, prop, out long value, out bool delete))
                    continue;

                if (delete)
                    _currentRun.DeleteCounter(docId, prop.Name);
                else
                    _currentRun.AddCounter(docId, prop.Name, value);
            }
        }
        
        private readonly struct TimeSeriesModifications
        {
            public readonly IEnumerable<SingleResult> ToAdd;
            public readonly IEnumerable<TimeSeriesDeletedRangeItem> ToDelete;

            public TimeSeriesModifications(IEnumerable<SingleResult> toAdd, IEnumerable<TimeSeriesDeletedRangeItem> toDelete)
            {
                ToAdd = toAdd;
                ToDelete = toDelete;
            }
        }
        
        private void AddAndRemoveTimeSeries(LazyStringValue docId, Dictionary<string, TimeSeriesModifications> modificationsPerTimeSeries)
        {
            foreach (var (timeSeriesName, modifications) in modificationsPerTimeSeries)
            {
                foreach (var singleResult in modifications.ToAdd)
                {
                    _currentRun.AddTimeSeries(docId, timeSeriesName, singleResult);
                }
                foreach (var range in modifications.ToDelete)
                {
                    _currentRun.RemoveTimeSeries(docId, timeSeriesName, range.From, range.To);
                }
            }
        }
        
        private void HandleSingleTimeSeriesSegment(string loadBehaviorFunction, EtlStatsScope stats, EtlProcessState state)
        {
            var docId = Current.DocumentId;
            var segmentEntry = Current.TimeSeriesSegmentEntry; 
            var doc = Database.DocumentsStorage.Get(Context, docId, DocumentFields.Default);
            if (doc == null)
            {
                //Through replication the Etl source database can have time-series without its document.
                //This is a rare situation and we will skip Etl this time-series and will mark the document so when it will be Etl we will send all its time-series with it
                (state.SkippedTimeSeriesDocs ??= new HashSet<string>()).Add(docId);
                return;
            }

            var timeSeriesEntries = segmentEntry.Segment.YieldAllValues(Context, segmentEntry.Start, false);
            if (loadBehaviorFunction != null && FilterSingleTimeSeriesSegmentByLoadBehaviorScript(ref timeSeriesEntries, docId, segmentEntry, loadBehaviorFunction))
                return;

            if (doc.Etag > segmentEntry.Etag)
            {
                //There is a chance that the document didn't Etl yet so we push it with the time-series to be sure
                doc = Database.DocumentsStorage.Get(Context, docId);

                if (DocumentScript != null)
                {
                    Current.Document = doc;
                    DocumentScript.Run(Context, Context, "execute", new object[] { doc }).Dispose();
                    if (_currentRun.IsDocumentLoadedToSameCollection(docId) == false)
                        return;
                }
                else
                {
                    _currentRun.PutFullDocument(docId, doc.Data);
                }
            }

            var timeSeriesName = Database.DocumentsStorage.TimeSeriesStorage.GetTimeSeriesNameOriginalCasing(Context, docId, segmentEntry.Name);
            
            foreach (var entry in timeSeriesEntries)
            {
                _currentRun.AddTimeSeries(docId, timeSeriesName, entry);
            }
        }
        
        private void HandleSingleTimeSeriesDeletedRangeItem(TimeSeriesDeletedRangeItem item, string loadBehaviorFunction)
        {
            TimeSeriesValuesSegment.ParseTimeSeriesKey(item.Key, Context, out var docId, out var name);

            if (loadBehaviorFunction != null)
            {
                if (ShouldFilterByScriptAndGetParams(docId, name, loadBehaviorFunction, out (DateTime begin, DateTime end)? toLoad)) 
                    return;
            
                if(toLoad.HasValue && (toLoad.Value.begin > item.To || toLoad.Value.end < item.From))
                    return;    
            }
            
            _currentRun.RemoveTimeSeries(docId, name, item.From, item.To);
        }

        private bool FilterSingleTimeSeriesSegmentByLoadBehaviorScript(
            ref IEnumerable<SingleResult> timeSeriesEntries, 
            LazyStringValue docId,
            TimeSeriesSegmentEntry segmentEntry, 
            string loadBehaviorFunction)
        {
            if (ShouldFilterByScriptAndGetParams(docId, segmentEntry.Name, loadBehaviorFunction, out (DateTime begin, DateTime end)? toLoad)) 
                return true;

            if (toLoad == null)
                return false;
            
            var lastTimestamp = segmentEntry.Segment.GetLastTimestamp(segmentEntry.Start);
            if (segmentEntry.Start > toLoad.Value.end || lastTimestamp < toLoad.Value.begin)
                return true;

            if (toLoad.Value.begin > segmentEntry.Start)
            {
                timeSeriesEntries = SkipUntilFrom(timeSeriesEntries, toLoad.Value.begin);
            }

            if (toLoad.Value.end < lastTimestamp)
            {
                timeSeriesEntries = BreakOnTo(timeSeriesEntries, toLoad.Value.end);
            }

            return false;
        }

        private static IEnumerable<SingleResult> SkipUntilFrom(IEnumerable<SingleResult> origin, DateTime from)
        {
            using var enumerator = origin.GetEnumerator();
            while (enumerator.MoveNext())
            {
                if (enumerator.Current.Timestamp >= @from)
                    yield return enumerator.Current;
            }
        }

        private static IEnumerable<SingleResult> BreakOnTo(IEnumerable<SingleResult> origin, DateTime to)
        {
            using var enumerator = origin.GetEnumerator();
            while (enumerator.MoveNext() && enumerator.Current.Timestamp <= to)
            {
                yield return enumerator.Current;
            }
        }

        private bool ShouldFilterByScriptAndGetParams(string docId, string timeSeriesName, string function, out (DateTime From, DateTime To)? toLoad)
        {
            toLoad = null;
            using (var scriptRunnerResult = BehaviorsScript.Run(Context, Context, function, new object[] {docId, timeSeriesName}))
            {
                if (scriptRunnerResult.BooleanValue != null)
                {
                    if (scriptRunnerResult.BooleanValue == false)
                        return true;
                }
                else if (scriptRunnerResult.IsNull)
                {
                    return true;
                }
                else if (scriptRunnerResult.Instance.IsObject() == false)
                {
                    throw new InvalidOperationException($"Return type of `{function}` function should be a boolean or object. docId({docId}), timeSeriesName({timeSeriesName})");
                }
                else
                {
                    var toLoadLocal = (From: DateTime.MinValue, To: DateTime.MaxValue);
                    foreach ((JsValue jsValue, PropertyDescriptor propertyDescriptor) in scriptRunnerResult.Instance.AsObject().GetOwnProperties())
                    {
                        var key = jsValue.AsString();
                        if (key.Equals("from", StringComparison.OrdinalIgnoreCase))
                        {
                            if (toLoadLocal.From != DateTime.MinValue)
                                throw new InvalidOperationException($"Duplicate of property `From`/`from`. docId({docId}), timeSeriesName({timeSeriesName}), function({function})");
                            toLoadLocal.From = propertyDescriptor.Value.AsDate().ToDateTime();
                        }
                        else if (key.Equals("to", StringComparison.OrdinalIgnoreCase))
                        {
                            if (toLoadLocal.To != DateTime.MaxValue)
                                throw new InvalidOperationException($"Duplicate of property `To`/`to`. docId({docId}), timeSeriesName({timeSeriesName}), function({function})");
                            toLoadLocal.To = propertyDescriptor.Value.AsDate().ToDateTime();
                        }
                        else
                        {
                            throw new InvalidOperationException($"Returned object should contain only `from` and `to` property but contain `{key}`. docId({docId}), timeSeriesName({timeSeriesName}), function({function})");
                        }
                    }

                    if (toLoadLocal.To < toLoadLocal.From)
                        throw new InvalidOperationException($"The property `from` is bigger the the `to` property. " +
                                                            $"docId{docId} timeSeries{timeSeriesName} from({toLoadLocal.From}) to({toLoadLocal.To})");
                    toLoad = toLoadLocal;
                }
            }

            return false;
        }

        private List<CounterOperation> GetCounterOperationsFor(RavenEtlItem item)
        {
            var counterOperations = new List<CounterOperation>();

            foreach (var cgd in Database.DocumentsStorage.CountersStorage.GetCounterValuesForDocument(Context, item.DocumentId))
            {
                if (cgd.Values.TryGet(CountersStorage.Values, out BlittableJsonReaderObject counters) == false)
                    return null;

                var prop = new BlittableJsonReaderObject.PropertyDetails();
                for (var i = 0; i < counters.Count; i++)
                {
                    counters.GetPropertyByIndex(i, ref prop);

                    if (GetCounterValueAndCheckIfShouldSkip(item.DocumentId, null, prop, out long value, out bool delete))
                        continue;

                    if (delete == false)
                    {
                        counterOperations.Add(new CounterOperation
                        {
                            Type = CounterOperationType.Put,
                            CounterName = prop.Name,
                            Delta = value
                        });
                    }
                    else
                    {
                        if (ShouldFilterOutDeletion(item))
                            continue;

                        counterOperations.Add(new CounterOperation
                        {
                            Type = CounterOperationType.Delete,
                            CounterName = prop.Name,
                        });
                    }
                }
            }

            return counterOperations;
        }
        
        private Dictionary<string, TimeSeriesModifications> GetTimeSeriesFor(RavenEtlItem item, string loadBehaviorFunction)
        {
            if ((Current.Document.Flags & DocumentFlags.HasTimeSeries) != DocumentFlags.HasTimeSeries)
                return null;
            
            if (item.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeriesNames) == false)
                return null;
            
            metadata.Modifications ??= new DynamicJsonValue(metadata);
            
            metadata.Modifications.Remove(Constants.Documents.Metadata.TimeSeries);
            
            var ret = new Dictionary<string, TimeSeriesModifications>();
            foreach (LazyStringValue timeSeriesName in timeSeriesNames)
            {
                (DateTime from, DateTime to)? toLoad = null;
                if(loadBehaviorFunction != null && ShouldFilterByScriptAndGetParams(item.DocumentId, timeSeriesName, loadBehaviorFunction, out toLoad))
                    continue;
                toLoad ??= (DateTime.MinValue, DateTime.MaxValue);
                
                var reader = Database.DocumentsStorage.TimeSeriesStorage.GetReader(Context, item.DocumentId, timeSeriesName, toLoad.Value.from, toLoad.Value.to);
                var deletedRanges = Database.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesForDoc(Context, item.DocumentId);
                ret[timeSeriesName] = new TimeSeriesModifications(reader.AllValues(), deletedRanges);
            }
            return ret;
        }
        private List<TimeSeriesOperation> GetTimeSeriesOperationsFor(RavenEtlItem item)
        {
            var modificationsPerTimeSeries = GetTimeSeriesFor(item, null);
            if (modificationsPerTimeSeries == null)
                return null; 
                
            var results = new List<TimeSeriesOperation>();
            
            foreach (var (timeSeriesName, modifications) in modificationsPerTimeSeries)
            {
                var operation = new TimeSeriesOperation { Name = timeSeriesName };
                
                foreach (var timeSeries in modifications.ToAdd)
                {
                    operation.Append(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = timeSeries.Tag,
                        Timestamp = timeSeries.Timestamp,
                        Values = timeSeries.Values.ToArray()
                    });
                }
                
                foreach (var range in modifications.ToDelete)
                {
                    (operation.Deletes ??= new List<TimeSeriesOperation.DeleteOperation>()).Add(new TimeSeriesOperation.DeleteOperation
                    {
                        From  = range.From, To = range.To
                    });
                }

                results.Add(operation);
            }
            return results;
        }
        
        private bool GetCounterValueAndCheckIfShouldSkip(LazyStringValue docId, string function, BlittableJsonReaderObject.PropertyDetails prop, out long value, out bool delete)
        {
            value = 0;

            if (prop.Value is LazyStringValue)
            {
                // a deleted counter is marked
                // with a change-vector string 

                delete = true;
            }

            else
            {
                delete = false;
                value = CountersStorage.InternalGetCounterValue(prop.Value as BlittableJsonReaderObject.RawBlob, docId, prop.Name);

                if (function != null)
                {
                    using (var result = BehaviorsScript.Run(Context, Context, function, new object[] { docId, prop.Name }))
                    {
                        if (result.BooleanValue != true)
                            return true;
                    }
                }
            }

            return false;
        }

        private IEnumerable<CounterGroupDetail> GetCounterGroupsFor(RavenEtlItem item)
        {
            return Database.DocumentsStorage.CountersStorage.GetCounterValuesForDocument(Context, item.DocumentId);
        }

        protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
        {
            _currentRun.LoadAttachment(reference, attachment);
        }

        protected override void AddLoadedCounter(JsValue reference, string name, long value)
        {
            _currentRun.LoadCounter(reference, name, value);
        }
        
        protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
        {
            _currentRun.LoadTimeSeries(reference, name, entries);
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

        private void ApplyDeleteCommands(RavenEtlItem item, OperationType operation, out bool isLoadedToDefaultCollectionDeleted)
        {
            // first, we need to delete docs prefixed by modified document ID to properly handle updates of 
            // documents loaded to non default collections
            isLoadedToDefaultCollectionDeleted = false;
            for (var i = 0; i < _script.LoadToCollections.Length; i++)
            {
                var collection = _script.LoadToCollections[i];

                if (_script.MayLoadToDefaultCollection(item, collection))
                {
                    if (operation != OperationType.Delete 
                        && _transformation.IsAddingAttachments == false 
                        && _transformation.Counters.IsAddingCounters == false 
                        && _transformation.TimeSeries.IsAddingTimeSeries == false
                        && _currentRun.IsDocumentLoadedToSameCollection(item.DocumentId))
                        continue;
                    _currentRun.Delete(new DeleteCommandData(item.DocumentId, null));
                    isLoadedToDefaultCollectionDeleted = true;
                }
                else
                {
                    _currentRun.Delete(new DeletePrefixedCommandData(GetPrefixedId(item.DocumentId, collection)));
                }
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
            
            private readonly Dictionary<string, string> _collectionToLoadTimeSeriesBehaviorFunction;

            private readonly Dictionary<string, string> _collectionToDeleteDocumentBehaviorFunction;

            public bool HasTransformation => Transformation != null;

            public bool HasLoadCounterBehaviors => _collectionToLoadCounterBehaviorFunction != null;
            
            public bool HasLoadTimeSeriesBehaviors => _collectionToLoadTimeSeriesBehaviorFunction != null;

            public bool HasDeleteDocumentsBehaviors => _collectionToDeleteDocumentBehaviorFunction != null;

            public ScriptInput(Transformation transformation)
            {
                DefaultCollections = new HashSet<string>(transformation.Collections, StringComparer.OrdinalIgnoreCase);

                if (string.IsNullOrWhiteSpace(transformation.Script))
                    return;

                if (transformation.IsEmptyScript == false)
                    Transformation = new PatchRequest(transformation.Script, PatchRequestType.RavenEtl);

                if (transformation.Counters.CollectionToLoadBehaviorFunction != null)
                    _collectionToLoadCounterBehaviorFunction = transformation.Counters.CollectionToLoadBehaviorFunction;

                if (transformation.TimeSeries.CollectionToLoadBehaviorFunction != null)
                    _collectionToLoadTimeSeriesBehaviorFunction = transformation.TimeSeries.CollectionToLoadBehaviorFunction;

                if (transformation.CollectionToDeleteDocumentsBehaviorFunction != null)
                    _collectionToDeleteDocumentBehaviorFunction = transformation.CollectionToDeleteDocumentsBehaviorFunction;

                if (HasLoadCounterBehaviors || HasDeleteDocumentsBehaviors || HasLoadTimeSeriesBehaviors)
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
                if(HasLoadCounterBehaviors)
                    return _collectionToLoadCounterBehaviorFunction.TryGetValue(collection, out functionName);
                functionName = null;
                return false;
            }
            
            public bool TryGetLoadTimeSeriesBehaviorFunctionFor(string collection, out string functionName)
            {
                if (HasLoadTimeSeriesBehaviors) 
                    return _collectionToLoadTimeSeriesBehaviorFunction.TryGetValue(collection, out functionName);
                functionName = null;
                return false;
            }

            public bool TryGetDeleteDocumentBehaviorFunctionFor(string collection, out string functionName)
            {
                return _collectionToDeleteDocumentBehaviorFunction.TryGetValue(collection, out functionName);
            }

            public bool MayLoadToDefaultCollection(RavenEtlItem item, string loadToCollection)
            {
                if (item.Collection != null)
                    return _collectionNameComparisons[item.Collection][loadToCollection];

                var collection = item.CollectionFromMetadata;

                return collection?.CompareTo(loadToCollection) == 0;
            }
            
            public bool MayLoadToDefaultCollection(RavenEtlItem item)
            {
                if (item.Collection == null)
                    return false;
                
                foreach (string loadToCollection in LoadToCollections)
                { 
                    if(_collectionNameComparisons.ContainsKey(loadToCollection))
                        return true;
                }

                return false;
            }
        }

        private enum OperationType
        {
            Put,
            Delete
        }
    }
}
