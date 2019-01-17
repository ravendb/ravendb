using System.Collections.Generic;
using System.Diagnostics;
using Jint.Native;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Counters;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlScriptRun
    {
        private readonly List<ICommandData> _deletes = new List<ICommandData>();

        private Dictionary<JsValue, (string Id, BlittableJsonReaderObject Document)> _putsByJsReference;
        
        private Dictionary<JsValue, List<(string Name, Attachment Attachment)>> _addAttachments;

        private Dictionary<JsValue, Attachment> _loadedAttachments;

        private Dictionary<JsValue, List<CounterOperation>> _countersByJsReference;

        private Dictionary<LazyStringValue, List<CounterOperation>> _countersByDocumentId;

        private Dictionary<JsValue, (string Name, long Value)> _loadedCountersByJsReference;

        private List<ICommandData> _fullDocuments;

        public void Delete(ICommandData command)
        {
            Debug.Assert(command is DeleteCommandData || command is DeletePrefixedCommandData);

            _deletes.Add(command);
        }

        public void PutFullDocument(string id, BlittableJsonReaderObject doc, List<Attachment> attachments, List<CounterOperation> counterOperations)
        {
            if (_fullDocuments == null)
                _fullDocuments = new List<ICommandData>();

            _fullDocuments.Add(new PutCommandDataWithBlittableJson(id, null, doc));

            if (attachments != null && attachments.Count > 0)
            {
                foreach (var attachment in attachments)
                {
                    _fullDocuments.Add(new PutAttachmentCommandData(id, attachment.Name, attachment.Stream, attachment.ContentType, null));
                }
            }

            if (counterOperations?.Count > 0)
            {
                _fullDocuments.Add(new CountersBatchCommandData(id, counterOperations)
                {
                    FromEtl = true
                });
            }
        }

        public void Put(string id, JsValue instance, BlittableJsonReaderObject doc)
        {
            Debug.Assert(instance != null);

            if (_putsByJsReference == null)
                _putsByJsReference = new Dictionary<JsValue, (string Id, BlittableJsonReaderObject)>(ReferenceEqualityComparer<JsValue>.Default);

            _putsByJsReference.Add(instance, (id, doc));
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
            
            return commands;
        }
    }
}
