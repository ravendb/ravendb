using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Documents.TimeSeries;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public abstract class RavenEtlScriptRun<T>
    where T : struct, IJsHandle<T>
    {
        private readonly IJsEngineHandle<T> _jsEngineHandle;
        private readonly EtlStatsScope _stats;
        private readonly List<ICommandData> _deletes = new List<ICommandData>();

        private Dictionary<T, (string Id, BlittableJsonReaderObject Document)> _putsByJsReference;
        
        private Dictionary<T, List<(string Name, Attachment Attachment)>> _addAttachments;

        private Dictionary<T, Attachment> _loadedAttachments;

        private Dictionary<T, List<CounterOperation>> _countersByJsReference;

        private Dictionary<LazyStringValue, List<CounterOperation>> _countersByDocumentId;
        
        private Dictionary<T, Dictionary<T, TimeSeriesOperation>> _timeSeriesByJsReference;

        private Dictionary<LazyStringValue, Dictionary<string, TimeSeriesBatchCommandData>> _timeSeriesByDocumentId;

        private Dictionary<T, (string Name, long Value)> _loadedCountersByJsReference;
        
        private Dictionary<T, (string Name, IEnumerable<SingleResult> Value)> _loadedTimeSeriesByJsReference;

        private Dictionary<string, List<ICommandData>> _fullDocuments;

        protected RavenEtlScriptRun(IJsEngineHandle<T> jsEngineHandle, EtlStatsScope stats)
        {
            _jsEngineHandle = jsEngineHandle;
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

        public void Put(string id, T instance, BlittableJsonReaderObject doc)
        {
            //TODO: egor Currently we cannot implement operator in interface
             Debug.Assert(instance.IsNull == false);

            _putsByJsReference ??= new Dictionary<T, (string Id, BlittableJsonReaderObject)>();

            _putsByJsReference.Add(instance, (id, doc));
            _stats.IncrementBatchSize(doc.Size);
        }

        public void LoadAttachment(T attachmentReference, Attachment attachment)
        {
            _loadedAttachments ??= new Dictionary<T, Attachment>();
            _loadedAttachments.Add(attachmentReference, attachment);
        }

        public void LoadCounter(T counterReference, string name, long value)
        {
            _loadedCountersByJsReference ??= new Dictionary<T, (string, long)>();
            _loadedCountersByJsReference.TryAdd(counterReference, (name, value));
        }
        
        public void LoadTimeSeries(T reference, string name, IEnumerable<SingleResult> value)
        {
            (_loadedTimeSeriesByJsReference ??= new Dictionary<T, (string, IEnumerable<SingleResult>)>())
                .TryAdd(reference, (name, value));
        }

        public void AddAttachment(T instance, string name, T attachmentReference)
        {
            var attachment = _loadedAttachments[attachmentReference];

            _addAttachments ??= new Dictionary<T, List<(string, Attachment)>>();

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

        public void AddCounter(T instance, T counterReference)
        {
            var counter = _loadedCountersByJsReference[counterReference];

            if (_countersByJsReference == null)
                _countersByJsReference = new Dictionary<T, List<CounterOperation>>();

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

        public void AddTimeSeries(T instance, T timeSeriesReference)
        {
            var (name, entries) = _loadedTimeSeriesByJsReference[timeSeriesReference];

            _timeSeriesByJsReference ??= new Dictionary<T, Dictionary<T, TimeSeriesOperation>>();
            if (_timeSeriesByJsReference.TryGetValue(instance, out var timeSeriesOperations) == false)
            {
                timeSeriesOperations = new Dictionary<T, TimeSeriesOperation>();
                _timeSeriesByJsReference.Add(instance, timeSeriesOperations);
            }

            var jsName = _jsEngineHandle.CreateValue(name);
            if (timeSeriesOperations.TryGetValue(jsName, out var timeSeriesOperation) == false)
            {
                timeSeriesOperation = new TimeSeriesOperation {Name = name};
                timeSeriesOperations.Add(jsName, timeSeriesOperation);
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
                            commands.Add(new TimeSeriesBatchCommandData(put.Value.Id, operation.Name, operation.Appends, operation.Deletes){FromEtl = true});
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

    public class RavenEtlScriptRunV8 : RavenEtlScriptRun<JsHandleV8> 
    {
        public RavenEtlScriptRunV8(IJsEngineHandle<JsHandleV8> engine, EtlStatsScope stats) : base(engine, stats)
        {
        }
    }

    public class RavenEtlScriptRunJint : RavenEtlScriptRun<JsHandleJint>
    {
        public RavenEtlScriptRunJint(IJsEngineHandle<JsHandleJint> engine, EtlStatsScope stats) : base(engine, stats)
        {
        }
    }
}
