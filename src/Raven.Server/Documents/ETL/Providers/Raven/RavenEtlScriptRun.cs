using System;
using System.Collections.Generic;
using System.Diagnostics;
using V8.Net;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.TimeSeries;
using Sparrow.Json;
using Raven.Server.Documents.Patch;
using Spatial4n.Core.Exceptions;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlScriptRun : IDisposable
    {
        private EtlStatsScope _stats;
        private List<ICommandData> _deletes = new List<ICommandData>();

        private const int _putByJsReferenceUndefinedId = Int32.MinValue;
        private int _putByJsReferencePrevId = _putByJsReferenceUndefinedId;
        private DictionaryCloningKeyJH<(string Id, BlittableJsonReaderObject Document)> _putsByJsReference;
        private DictionaryCloningKeyJH<int> _putsById;
        
        private DictionaryCloningKeyJH<List<(string Name, Attachment Attachment)>> _addAttachmentsByJsReference;
        private Dictionary<int, List<(string Name, Attachment Attachment)>> _addAttachmentsById;

        private Dictionary<string, Attachment> _loadedAttachments;

        private DictionaryCloningKeyJH<List<CounterOperation>> _countersByJsReference;
        private Dictionary<int, List<CounterOperation>> _countersById;

        private Dictionary<LazyStringValue, List<CounterOperation>> _countersByDocumentId;
        
        private DictionaryCloningKeyJH<Dictionary<string, TimeSeriesOperation>> _timeSeriesByJsReference;
        private Dictionary<int, Dictionary<string, TimeSeriesOperation>> _timeSeriesById;

        private Dictionary<LazyStringValue, Dictionary<string, TimeSeriesBatchCommandData>> _timeSeriesByDocumentId;

        private Dictionary<string, (string Name, long Value)> _loadedCountersByJsReference;
        
        private Dictionary<string, (string Name, IEnumerable<SingleResult> Value)> _loadedTimeSeriesByJsReference;

        private Dictionary<string, List<ICommandData>> _fullDocuments;

        private bool _disposed;
        
        public RavenEtlScriptRun(EtlStatsScope stats)
        {
            _stats = stats;
            _disposed = false;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            _stats = null;

            _deletes?.Clear();
            _deletes = null;

            _putByJsReferencePrevId = _putByJsReferenceUndefinedId;
            _putsByJsReference?.Dispose();
            _addAttachmentsByJsReference?.Clear();
            _addAttachmentsById?.Clear();
            _loadedAttachments?.Clear();

            _countersByJsReference?.Clear();
            _countersById?.Clear();

            _countersByDocumentId?.Clear();
            _countersByDocumentId = null;

            _timeSeriesByJsReference?.Clear();
            _timeSeriesById?.Clear();

            _timeSeriesByDocumentId?.Clear();
            _timeSeriesByDocumentId = null;

            _loadedCountersByJsReference?.Clear();
            _loadedTimeSeriesByJsReference?.Clear();

            _fullDocuments?.Clear();
            _fullDocuments = null;

            _disposed = true;
        }

        public void Delete(ICommandData command)
        {
            Debug.Assert(command is DeleteCommandData || command is DeletePrefixedCommandData);

            _deletes.Add(command);
        }

        public void PutFullDocument(
            string id, 
            BlittableJsonReaderObject doc, 
            List<Attachment> attachments = null, 
            List<CounterOperation> counterOperations = null, 
            List<TimeSeriesOperation> timeSeriesOperations = null)
        {
            _fullDocuments ??= new Dictionary<string, List<ICommandData>>();

            if (_fullDocuments.ContainsKey(id))
                return;

            var commands = _fullDocuments[id] = new List<ICommandData>();

            commands.Add(new PutCommandDataWithBlittableJson(id, null,null, doc));

            _stats.IncrementBatchSize(doc.Size);

            if (attachments != null && attachments.Count > 0)
            {
                foreach (var attachment in attachments)
                {
                    commands.Add(new PutAttachmentCommandData(id, attachment.Name, attachment.Stream, attachment.ContentType, null));

                    _stats.IncrementBatchSize(attachment.Stream.Length);
                }
            }

            if (counterOperations?.Count > 0)
            {
                commands.Add(new CountersBatchCommandData(id, counterOperations)
                {
                    FromEtl = true
                });
            }
            
            if (timeSeriesOperations != null)
            {
                foreach (var operation in timeSeriesOperations)
                {
                    commands.Add(new TimeSeriesBatchCommandData(id, operation.Name, operation.Appends, operation.Deletes)
                    {
                        FromEtl = true
                    });    
                }
            }
        }

        public void Put(string id, JsHandle instance, BlittableJsonReaderObject doc)
        {
            Debug.Assert(!instance.IsEmpty);

            _putsByJsReference ??= new DictionaryCloningKeyJH<(string Id, BlittableJsonReaderObject)>();

            if (instance.EngineType == JavaScriptEngineType.V8)
            {
                var instanceV8 = instance.V8.Item;
                var engine = instanceV8.Engine;
                if (instanceV8.IsObject && instanceV8.ObjectID < 0)
                {
                    // RavenDB_7064.Should_handle_attachments
                    // native objects (having instanceV8.ObjectID < -1) used as keys in *ByJsReference in case of V8 they get different handle and object ids in sequential call backs
                    // so they get to be not equal (they are compared on handle id) and can not be used as keys 
                    // while for managed objects (having instanceV8.ObjectID >= 0) handle id remains the same
                    // thus, for native objects we store special $putsByJsReferenceId property and use it as key in dictionaries *ById
                    // in the end we remove this property
                    if (_putByJsReferencePrevId == Int32.MaxValue)
                    {
                        throw new RuntimeException("_putByJsReferenceCurrentId has maximum value and can not be increased any more");
                    }

                    _putByJsReferencePrevId += 1;
                    instanceV8.SetPropertyOrThrow("$putsByJsReferenceId", engine.CreateValue(_putByJsReferencePrevId));
                }
            }

            _putsByJsReference.Add(instance, (id, doc));
            _stats.IncrementBatchSize(doc.Size);
        }

        public void LoadAttachment(JsHandle attachmentReference, Attachment attachment)
        {
            if (!attachmentReference.IsString)
                throw new ArgumentException($"Invalid attachmentReference type {attachmentReference.ValueType}");

            _loadedAttachments ??= new Dictionary<string, Attachment>();
            _loadedAttachments.Add(attachmentReference.AsString, attachment);
        }

        public void LoadCounter(JsHandle counterReference, string name, long value)
        {
            if (!counterReference.IsString)
                throw new ArgumentException($"Invalid attachmentReference type {counterReference.ValueType}");
            
            _loadedCountersByJsReference ??= new Dictionary<string, (string, long)>();
            _loadedCountersByJsReference.TryAdd(counterReference.AsString, (name, value));
        }
        
        public void LoadTimeSeries(JsHandle reference, string name, IEnumerable<SingleResult> value)
        {
            if (!reference.IsString)
                throw new ArgumentException($"Invalid attachmentReference type {reference.ValueType}");

            (_loadedTimeSeriesByJsReference ??= new Dictionary<string, (string, IEnumerable<SingleResult>)>())
                .TryAdd(reference.AsString, (name, value));
        }

        public void AddAttachment(JsHandle instance, string name, JsHandle attachmentReference)
        {
            if (!attachmentReference.IsString)
                throw new ArgumentException($"Invalid attachmentReference type {attachmentReference.ValueType}");
            
            var attachment = _loadedAttachments[attachmentReference.AsString];

            _addAttachmentsByJsReference ??= new DictionaryCloningKeyJH<List<(string Name, Attachment Attachment)>>();
            _addAttachmentsById ??= new Dictionary<int, List<(string Name, Attachment Attachment)>>();

            var attachments = AddItemBuyPutKeyOrId(instance, _addAttachmentsByJsReference, _addAttachmentsById);
            attachments.Add((name ?? attachment.Name, attachment));
            _stats.IncrementBatchSize(attachment.Stream.Length);
        }

        public void DeleteAttachment(string documentId, string name)
        {
            _deletes.Add(new DeleteAttachmentCommandData(documentId, name, null));
        }

        public void AddCounter(JsHandle instance, JsHandle counterReference)
        {
            if (!counterReference.IsString)
                throw new ArgumentException($"Invalid attachmentReference type {counterReference.ValueType}");
            
            var counter = _loadedCountersByJsReference[counterReference.AsString];

            _countersByJsReference ??= new DictionaryCloningKeyJH<List<CounterOperation>>();
            _countersById ??= new Dictionary<int, List<CounterOperation>>();

            var operations = AddItemBuyPutKeyOrId(instance, _countersByJsReference, _countersById);
            operations.Add(new CounterOperation
            {
                CounterName = counter.Name,
                Delta = counter.Value,
                Type = CounterOperationType.Put
            });
        }

        public void AddCounter(LazyStringValue documentId, string counterName, long value)
        {
            if (_countersByDocumentId == null)
                _countersByDocumentId = new Dictionary<LazyStringValue, List<CounterOperation>>(LazyStringValueComparer.Instance);

            if (_countersByDocumentId.TryGetValue(documentId, out var counters) == false)
            {
                counters = new List<CounterOperation>();
                _countersByDocumentId.Add(documentId, counters);
            }

            counters.Add(new CounterOperation
            {
                CounterName = counterName,
                Delta = value,
                Type = CounterOperationType.Put
            });
        }

        public void DeleteCounter(LazyStringValue documentId, string counterName)
        {
            if (_countersByDocumentId == null)
                _countersByDocumentId = new Dictionary<LazyStringValue, List<CounterOperation>>(LazyStringValueComparer.Instance);

            if (_countersByDocumentId.TryGetValue(documentId, out var counters) == false)
            {
                counters = new List<CounterOperation>();
                _countersByDocumentId.Add(documentId, counters);
            }

            counters.Add(new CounterOperation
            {
                CounterName = counterName,
                Type = CounterOperationType.Delete
            });
        }

        public void AddTimeSeries(JsHandle instance, JsHandle timeSeriesReference)
        {
            if (!timeSeriesReference.IsString)
                throw new ArgumentException($"Invalid attachmentReference type {timeSeriesReference.ValueType}");
            
            var (name, entries) = _loadedTimeSeriesByJsReference[timeSeriesReference.AsString];

            _timeSeriesByJsReference ??= new DictionaryCloningKeyJH<Dictionary<string, TimeSeriesOperation>>();
            _timeSeriesById ??= new Dictionary<int, Dictionary<string, TimeSeriesOperation>>();

            var timeSeriesOperations = AddItemBuyPutKeyOrId(instance, _timeSeriesByJsReference, _timeSeriesById);
            if (timeSeriesOperations.TryGetValue(name, out var timeSeriesOperation) == false)
            {
                timeSeriesOperation = new TimeSeriesOperation {Name = name};
                timeSeriesOperations.Add(name, timeSeriesOperation);
            }

            foreach (var entry in entries)
            {
                timeSeriesOperation.Append(new TimeSeriesOperation.AppendOperation
                {
                    Timestamp = entry.Timestamp,
                    Tag = entry.Tag,
                    Values = entry.Values.ToArray()
                });    
            }
        }
        
        public void AddTimeSeries(LazyStringValue documentId, string timeSeriesName, SingleResult timeSeries)
        {
            var timeSeriesOperation = GetTimeSeriesOperationFor(documentId, timeSeriesName);
            timeSeriesOperation.TimeSeries.Append(new TimeSeriesOperation.AppendOperation
            {
                Timestamp = timeSeries.Timestamp,
                Tag = timeSeries.Tag,
                Values = timeSeries.Values.ToArray(),
            });
        }

        public void RemoveTimeSeries(LazyStringValue documentId, string timeSeriesName, DateTime from, DateTime to)
        {
            var timeSeriesOperation = GetTimeSeriesOperationFor(documentId, timeSeriesName);
            (timeSeriesOperation.TimeSeries.Deletes ??= new List<TimeSeriesOperation.DeleteOperation>())
                .Add(new TimeSeriesOperation.DeleteOperation { From = from, To = to });
        }

        private TimeSeriesBatchCommandData GetTimeSeriesOperationFor(LazyStringValue documentId, string timeSeriesName)
        {
            _timeSeriesByDocumentId ??= new Dictionary<LazyStringValue, Dictionary<string, TimeSeriesBatchCommandData>>(LazyStringValueComparer.Instance);
            if (_timeSeriesByDocumentId.TryGetValue(documentId, out var timeSeriesOperations) == false)
            {
                timeSeriesOperations = new Dictionary<string, TimeSeriesBatchCommandData>();
                _timeSeriesByDocumentId.Add(documentId, timeSeriesOperations);
            }

            if (timeSeriesOperations.TryGetValue(timeSeriesName, out var timeSeriesOperation) == false)
            {
                timeSeriesOperation = new TimeSeriesBatchCommandData(documentId, timeSeriesName, appends: null, deletes: null);
                timeSeriesOperations.Add(timeSeriesName, timeSeriesOperation);
            }

            return timeSeriesOperation;
        }


        private T AddItemBuyPutKeyOrId<T>(JsHandle putKey, DictionaryCloningKeyJH<T> _itemsByPutKey, Dictionary<int, T> _itemsById)
        where T : new()
        {
            int putId = _putByJsReferenceUndefinedId;
            if (putKey.EngineType == JavaScriptEngineType.V8)
            {
                var instanceV8 = putKey.V8.Item;
                if (instanceV8.IsObject && instanceV8.ObjectID < 0)
                {
                    var jsId = instanceV8.GetProperty("$putsByJsReferenceId");
                    if (jsId.IsInt32)
                    {
                        putId = jsId.AsInt32;
                    }
                    else
                    {
                        throw new RuntimeException("Property $putsByJsReferenceId has not been found on the V8 instance"); 
                    }
                }
            }

            T items;
            if (putId > _putByJsReferenceUndefinedId)
            {
                if (_itemsById.TryGetValue(putId, out items) == false)
                {
                    items = new T();
                    _itemsById.Add(putId, items);
                }
            }
            else
            {
                if (_itemsByPutKey.TryGetValue(putKey, out items) == false)
                {
                    items = new T();
                    _itemsByPutKey.Add(putKey, items);
                }
            }

            return items;
        }

        private T GetItemBuyPutKeyOrId<T>(JsHandle putKey, int putId, DictionaryCloningKeyJH<T> items, Dictionary<int, T> _itemsById)
        where T : class
        {
            T res = null;
            if (putId > _putByJsReferenceUndefinedId)
            {
                _itemsById.TryGetValue(putId, out res);
            }
            else
            {
                items.TryGetValue(putKey, out res);
            }
            return res;
        }

        public List<ICommandData> GetCommands()
        {
            // let's send deletions first
            var commands = _deletes;

            if (_fullDocuments != null)
            {
                foreach (var command in _fullDocuments)
                {
                    commands.AddRange(command.Value);
                }
            }

            if (_putsByJsReference != null)
            {
                foreach (var put in _putsByJsReference)
                {
                    commands.Add(new PutCommandDataWithBlittableJson(put.Value.Id, null, null, put.Value.Document));

                    var putKey = put.Key;
                    var putId = _putByJsReferenceUndefinedId;
                    if (putKey.EngineType == JavaScriptEngineType.V8)
                    {
                        var instanceV8 = putKey.V8.Item;
                        if (instanceV8.IsObject && instanceV8.ObjectID < 0)
                        {
                            var jsId = instanceV8.GetProperty("$putsByJsReferenceId");
                            if (jsId.IsInt32)
                            {
                                putId = jsId.AsInt32;
                            }
                            else
                            {
                                throw new RuntimeException("Property $putsByJsReferenceId has not been found on the V8 instance"); 
                            }
                        }
                    }

                    if (_addAttachmentsByJsReference != null)
                    {
                        var putAttachments = GetItemBuyPutKeyOrId<List<(string Name, Attachment Attachment)>>(putKey, putId, _addAttachmentsByJsReference, _addAttachmentsById);                        
                        if (putAttachments != null)
                        {
                            foreach (var addAttachment in putAttachments)
                            {
                                commands.Add(new PutAttachmentCommandData(put.Value.Id, addAttachment.Name, addAttachment.Attachment.Stream,
                                    addAttachment.Attachment.ContentType,
                                    null));
                            }
                        }
                    }

                    if (_countersByJsReference != null)
                    {
                        var counterOperations = GetItemBuyPutKeyOrId<List<CounterOperation>>(putKey, putId, _countersByJsReference, _countersById);
                        if (counterOperations != null)
                        {
                            commands.Add(new CountersBatchCommandData(put.Value.Id, counterOperations) {FromEtl = true});
                        }
                    }

                    if (_timeSeriesByJsReference != null)
                    {
                        var timeSeriesOperations = GetItemBuyPutKeyOrId<Dictionary<string, TimeSeriesOperation>>(putKey, putId, _timeSeriesByJsReference, _timeSeriesById);
                        if (timeSeriesOperations != null)
                        {
                            foreach (var (_, operation) in timeSeriesOperations)
                            {
                                commands.Add(new TimeSeriesBatchCommandData(put.Value.Id, operation.Name, operation.Appends, operation.Deletes) {FromEtl = true});
                            }
                        }
                    }

                    if (putId > _putByJsReferenceUndefinedId)
                    {
                        putKey.DeleteProperty("$putsByJsReferenceId", throwOnError: true);
                    }
                }
            }

            if (_countersByDocumentId != null)
            {
                foreach (var counter in _countersByDocumentId)
                {
                    commands.Add(new CountersBatchCommandData(counter.Key, counter.Value)
                    {
                        FromEtl = true
                    });
                }
            }
            
            if (_timeSeriesByDocumentId != null)
            {
                foreach (var timeSeriesSetForDoc in _timeSeriesByDocumentId)
                {
                    foreach (var value in timeSeriesSetForDoc.Value.Values)
                    {
                        value.FromEtl = true;
                        commands.Add(value);
                    }
                }
            }
            
            return commands;
        }

        public bool IsDocumentLoadedToSameCollection(LazyStringValue documentId)
        {
            if (_putsByJsReference != null)
            {
                foreach (var (_, (id, _)) in _putsByJsReference)
                {
                    if (id == documentId)
                        return true;
                }
            }

            return false;
        }
    }
}
