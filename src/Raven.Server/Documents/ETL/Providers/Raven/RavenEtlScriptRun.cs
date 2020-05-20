using System;
using System.Collections.Generic;
using System.Diagnostics;
using Jint.Native;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.TimeSeries;
using Sparrow.Json;
using Sparrow.Utils;
using TimeSeriesStats = Raven.Server.Documents.TimeSeries.TimeSeriesStats;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlScriptRun
    {
        private readonly EtlStatsScope _stats;
        private readonly List<ICommandData> _deletes = new List<ICommandData>();

        private Dictionary<JsValue, (string Id, BlittableJsonReaderObject Document)> _putsByJsReference;
        
        private Dictionary<JsValue, List<(string Name, Attachment Attachment)>> _addAttachments;

        private Dictionary<JsValue, Attachment> _loadedAttachments;

        private Dictionary<JsValue, List<CounterOperation>> _countersByJsReference;

        private Dictionary<LazyStringValue, List<CounterOperation>> _countersByDocumentId;
        
        private Dictionary<JsValue, Dictionary<JsValue, TimeSeriesOperation>> _timeSeriesByJsReference;

        private Dictionary<LazyStringValue, Dictionary<string, TimeSeriesBatchCommandData>> _timeSeriesByDocumentId;

        private Dictionary<JsValue, (string Name, long Value)> _loadedCountersByJsReference;
        private Dictionary<JsValue, (string Name, IEnumerable<TimeSeriesStorage.Reader.SingleResult> Value)> _loadedTimeSeriesByJsReference;

        private List<ICommandData> _fullDocuments;

        public RavenEtlScriptRun(EtlStatsScope stats)
        {
            _stats = stats;
        }

        public void Delete(ICommandData command)
        {
            Debug.Assert(command is DeleteCommandData || command is DeletePrefixedCommandData);

            _deletes.Add(command);
        }

        public void PutFullDocument(
            string id, 
            BlittableJsonReaderObject doc, 
            List<Attachment> attachments, 
            List<CounterOperation> counterOperations, 
            List<TimeSeriesOperation> timeSeriesOperations)
        {
            if (_fullDocuments == null)
                _fullDocuments = new List<ICommandData>();

            _fullDocuments.Add(new PutCommandDataWithBlittableJson(id, null, doc));

            _stats.IncrementBatchSize(doc.Size);

            if (attachments != null && attachments.Count > 0)
            {
                foreach (var attachment in attachments)
                {
                    _fullDocuments.Add(new PutAttachmentCommandData(id, attachment.Name, attachment.Stream, attachment.ContentType, null));

                    _stats.IncrementBatchSize(attachment.Stream.Length);
                }
            }

            if (counterOperations?.Count > 0)
            {
                _fullDocuments.Add(new CountersBatchCommandData(id, counterOperations)
                {
                    FromEtl = true
                });
            }
            
            if (timeSeriesOperations != null)
            {
                foreach (var operation in timeSeriesOperations)
                {
                    _fullDocuments.Add(new TimeSeriesBatchCommandData(id, operation.Name, operation.Appends, operation.Removals)
                    {
                        //TODO      FromEtl = true
                    });    
                }
            }
        }

        public void Put(string id, JsValue instance, BlittableJsonReaderObject doc)
        {
            Debug.Assert(instance != null);

            if (_putsByJsReference == null)
                _putsByJsReference = new Dictionary<JsValue, (string Id, BlittableJsonReaderObject)>(ReferenceEqualityComparer<JsValue>.Default);

            _putsByJsReference.Add(instance, (id, doc));
            _stats.IncrementBatchSize(doc.Size);
        }

        public void LoadAttachment(JsValue attachmentReference, Attachment attachment)
        {
            if (_loadedAttachments == null)
                _loadedAttachments = new Dictionary<JsValue, Attachment>(ReferenceEqualityComparer<JsValue>.Default);

            _loadedAttachments.Add(attachmentReference, attachment);
        }

        public void LoadCounter(JsValue counterReference, string name, long value)
        {
            if (_loadedCountersByJsReference == null)
                _loadedCountersByJsReference = new Dictionary<JsValue, (string, long)>(ReferenceEqualityComparer<JsValue>.Default);

            _loadedCountersByJsReference.Add(counterReference, (name, value));
        }
        
        public void LoadTimeSeries(JsValue reference, string name, IEnumerable<TimeSeriesStorage.Reader.SingleResult> value)
        {
            (_loadedTimeSeriesByJsReference ??= new Dictionary<JsValue, (string, IEnumerable<TimeSeriesStorage.Reader.SingleResult>)>(ReferenceEqualityComparer<JsValue>.Default))
                .Add(reference, (name, value));
        }

        public void AddAttachment(JsValue instance, string name, JsValue attachmentReference)
        {
            var attachment = _loadedAttachments[attachmentReference];

            if (_addAttachments == null)
                _addAttachments = new Dictionary<JsValue, List<(string, Attachment)>>(ReferenceEqualityComparer<JsValue>.Default);

            if (_addAttachments.TryGetValue(instance, out var attachments) == false)
            {
                attachments = new List<(string, Attachment)>();
                _addAttachments.Add(instance, attachments);
            }

            attachments.Add((name ?? attachment.Name, attachment));
            _stats.IncrementBatchSize(attachment.Stream.Length);
        }

        public void DeleteAttachment(string documentId, string name)
        {
            _deletes.Add(new DeleteAttachmentCommandData(documentId, name, null));
        }

        public void AddCounter(JsValue instance, JsValue counterReference)
        {
            var counter = _loadedCountersByJsReference[counterReference];

            if (_countersByJsReference == null)
                _countersByJsReference = new Dictionary<JsValue, List<CounterOperation>>(ReferenceEqualityComparer<JsValue>.Default);

            if (_countersByJsReference.TryGetValue(instance, out var operations) == false)
            {
                operations = new List<CounterOperation>();
                _countersByJsReference.Add(instance, operations);
            }

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

        public void AddTimeSeries(JsValue instance, JsValue timeSeriesReference)
        {
            var (name, entries) = _loadedTimeSeriesByJsReference[timeSeriesReference];
            
            _timeSeriesByJsReference ??= new Dictionary<JsValue, Dictionary<JsValue, TimeSeriesOperation>>(ReferenceEqualityComparer<JsValue>.Default);
            if (_timeSeriesByJsReference.TryGetValue(instance, out var timeSeriesOperations) == false)
            {
                timeSeriesOperations = new Dictionary<JsValue, TimeSeriesOperation>();
                _timeSeriesByJsReference.Add(instance, timeSeriesOperations);
            }

            if (timeSeriesOperations.TryGetValue(name, out var timeSeriesOperation) == false)
            {
                timeSeriesOperation = new TimeSeriesOperation();
                timeSeriesOperations.Add(name, timeSeriesOperation);
            }

            timeSeriesOperation.Appends ??= new List<TimeSeriesOperation.AppendOperation>();
            foreach (var entry in entries)
            {
                timeSeriesOperation.Appends.Add(new TimeSeriesOperation.AppendOperation
                {
                    Timestamp = entry.Timestamp,
                    Tag = entry.Tag,
                    Values = entry.Values.ToArray()
                });    
            }
        }
        
        public void AddTimeSeries(LazyStringValue documentId, string timeSeriesName, TimeSeriesStorage.Reader.SingleResult timeSeries)
        {
            var timeSeriesOperation = GetTimeSeriesOperationFor(documentId, timeSeriesName);
            (timeSeriesOperation.TimeSeries.Appends ??= new List<TimeSeriesOperation.AppendOperation>())
                .Add(new TimeSeriesOperation.AppendOperation
                {
                    Timestamp = timeSeries.Timestamp,
                    Tag = timeSeries.Tag,
                    Values = timeSeries.Values.ToArray()
                });
        }

        public void RemoveTimeSeries(LazyStringValue documentId, string timeSeriesName, DateTime from, DateTime to)
        {
            var timeSeriesOperation = GetTimeSeriesOperationFor(documentId, timeSeriesName);
            (timeSeriesOperation.TimeSeries.Removals ??= new List<TimeSeriesOperation.RemoveOperation>())
                .Add(new TimeSeriesOperation.RemoveOperation { From = from, To = to });
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
                timeSeriesOperation = new TimeSeriesBatchCommandData(documentId, timeSeriesName, null, null);
                timeSeriesOperations.Add(timeSeriesName, timeSeriesOperation);
            }

            return timeSeriesOperation;
        }


        //         public void AddTimeSeries(LazyStringValue documentId, string timeSeriesName, IEnumerable<TimeSeriesStorage.Reader.SingleResult> entries)
        // {
        //     _timeSeriesByDocumentId ??= new Dictionary<LazyStringValue, Dictionary<string, TimeSeriesBatchCommandData>>(LazyStringValueComparer.Instance);
        //     if (_timeSeriesByDocumentId.TryGetValue(documentId, out var timeSeriesOperations) == false)
        //     {
        //         timeSeriesOperations = new Dictionary<string, TimeSeriesBatchCommandData>();
        //         _timeSeriesByDocumentId.Add(documentId, timeSeriesOperations);
        //     }
        //
        //     if (timeSeriesOperations.TryGetValue(timeSeriesName, out var timeSeriesOperation) == false)
        //     {
        //         timeSeriesOperation = new TimeSeriesBatchCommandData(documentId, timeSeriesName, null, null);
        //         timeSeriesOperations.Add(documentId, timeSeriesOperation);
        //     }
        //     
        //     ref var appends = ref timeSeriesOperation.TimeSeries.Appends;
        //     ref var removals = ref timeSeriesOperation.TimeSeries.Removals;
        //     (DateTime from, DateTime to) toRemove = (default, default); 
        //     foreach (var entry in entries)
        //     {
        //         if (entry.Status == TimeSeriesValuesSegment.Dead)
        //         {
        //             if (toRemove.from == default)
        //                 toRemove.from = entry.Timestamp;
        //             toRemove.to = entry.Timestamp;
        //         }
        //         else
        //         {
        //             CheckAndAddToRemovals(ref removals);
        //             appends ??= new List<TimeSeriesOperation.AppendOperation>();
        //             appends.Add(new TimeSeriesOperation.AppendOperation
        //             {
        //                 Timestamp = entry.Timestamp,
        //                 Tag = entry.Tag,
        //                 Values = entry.Values.ToArray()
        //             });
        //         }
        //     }
        //     CheckAndAddToRemovals(ref removals);
        //
        //     void CheckAndAddToRemovals(ref List<TimeSeriesOperation.RemoveOperation> removals)
        //     {
        //         if (toRemove.from == default)
        //             return;
        //         
        //         removals ??= new List<TimeSeriesOperation.RemoveOperation>();
        //         removals.Add(new TimeSeriesOperation.RemoveOperation {From = toRemove.from, To = toRemove.to});
        //         toRemove.from = default;
        //     }
        // }

        public List<ICommandData> GetCommands()
        {
            // let's send deletions first
            var commands = _deletes;

            if (_fullDocuments != null)
            {
                foreach (var command in _fullDocuments)
                {
                    commands.Add(command);
                }
            }

            if (_putsByJsReference != null)
            {
                foreach (var put in _putsByJsReference)
                {
                    commands.Add(new PutCommandDataWithBlittableJson(put.Value.Id, null, put.Value.Document));

                    if (_addAttachments != null && _addAttachments.TryGetValue(put.Key, out var putAttachments))
                    {
                        foreach (var addAttachment in putAttachments)
                        {
                            commands.Add(new PutAttachmentCommandData(put.Value.Id, addAttachment.Name, addAttachment.Attachment.Stream, addAttachment.Attachment.ContentType,
                                null));
                        }
                    }

                    if (_countersByJsReference != null && _countersByJsReference.TryGetValue(put.Key, out var counterOperations))
                    {
                        commands.Add(new CountersBatchCommandData(put.Value.Id, counterOperations)
                        {
                            FromEtl = true
                        });
                    }
                    
                    if (_timeSeriesByJsReference != null && _timeSeriesByJsReference.TryGetValue(put.Key, out var timeSeriesOperations))
                    {
                        foreach (var (_, operation) in timeSeriesOperations)
                        {
                            commands.Add(new TimeSeriesBatchCommandData(put.Value.Id, operation.Name, operation.Appends, operation.Removals));
                        }
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
                    commands.AddRange(timeSeriesSetForDoc.Value.Values);
                }
            }
            
            return commands;
        }
    }
}
