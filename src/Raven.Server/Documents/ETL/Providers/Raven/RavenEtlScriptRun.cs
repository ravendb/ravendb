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

        private Dictionary<JsValue, (string Id, BlittableJsonReaderObject Document)> _puts;
        
        private Dictionary<JsValue, List<(string Name, Attachment Attachment)>> _addAttachments;

        private Dictionary<JsValue, List<(string Name, long Value)>> _addCounters;

        private Dictionary<string, List<CounterOperation>> _counters;

        private Dictionary<JsValue, Attachment> _loadedAttachments;

        private Dictionary<JsValue, (string Name, long Value)> _loadedCounters;

        private List<ICommandData> _docsAndAttachments;

        public void Delete(ICommandData command)
        {
            Debug.Assert(command is DeleteCommandData || command is DeletePrefixedCommandData);

            _deletes.Add(command);
        }

        public void PutDocumentAndAttachments(string id, BlittableJsonReaderObject doc, List<Attachment> attachments)
        {
            if (_docsAndAttachments == null)
                _docsAndAttachments = new List<ICommandData>();

            _docsAndAttachments.Add(new PutCommandDataWithBlittableJson(id, null, doc));

            if (attachments != null && attachments.Count > 0)
            {
                foreach (var attachment in attachments)
                {
                    _docsAndAttachments.Add(new PutAttachmentCommandData(id, attachment.Name, attachment.Stream, attachment.ContentType, null));
                }
            }
        }

        public void Put(string id, JsValue instance, BlittableJsonReaderObject doc)
        {
            Debug.Assert(instance != null);

            if (_puts == null)
                _puts = new Dictionary<JsValue, (string Id, BlittableJsonReaderObject)>(ReferenceEqualityComparer<JsValue>.Default);

            _puts.Add(instance, (id, doc));
        }

        public void LoadAttachment(JsValue attachmentReference, Attachment attachment)
        {
            if (_loadedAttachments == null)
                _loadedAttachments = new Dictionary<JsValue, Attachment>(ReferenceEqualityComparer<JsValue>.Default);

            _loadedAttachments.Add(attachmentReference, attachment);
        }

        public void LoadCounter(JsValue counterReference, string name, long value)
        {
            if (_loadedCounters == null)
                _loadedCounters = new Dictionary<JsValue, (string, long)>(ReferenceEqualityComparer<JsValue>.Default);

            _loadedCounters.Add(counterReference, (name, value));
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

        public void AddCounter(JsValue instance, string name, JsValue counterReference)
        {
            var counter = _loadedCounters[counterReference];

            if (_addCounters == null)
                _addCounters = new Dictionary<JsValue, List<(string, long)>>(ReferenceEqualityComparer<JsValue>.Default);

            if (_addCounters.TryGetValue(instance, out var counters) == false)
            {
                counters = new List<(string, long)>();
                _addCounters.Add(instance, counters);
            }

            counters.Add((name ?? counter.Name, counter.Value));
        }

        public void AddCounter(string documentId, string counterName, long value)
        {
            if (_counters == null)
                _counters = new Dictionary<string, List<CounterOperation>>();

            if (_counters.TryGetValue(documentId, out var counters) == false)
            {
                counters = new List<CounterOperation>();
                _counters.Add(documentId, counters);
            }

            counters.Add(new CounterOperation
            {
                CounterName = counterName,
                Delta = value,
                Type = CounterOperationType.Put
            });
        }

        public void DeleteCounter(string documentId, string counterName)
        {
            if (_counters == null)
                _counters = new Dictionary<string, List<CounterOperation>>();

            if (_counters.TryGetValue(documentId, out var counters) == false)
            {
                counters = new List<CounterOperation>();
                _counters.Add(documentId, counters);
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

            if (_docsAndAttachments != null)
            {
                foreach (var command in _docsAndAttachments)
                {
                    commands.Add(command);
                }
            }

            if (_puts != null)
            {
                foreach (var put in _puts)
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

                    if (_addCounters != null && _addCounters.TryGetValue(put.Key, out var counters))
                    {
                        var counterOperations = new List<CounterOperation>();

                        foreach (var counter in counters)
                        {
                            counterOperations.Add(new CounterOperation()
                            {
                                Type = CounterOperationType.Put,
                                CounterName = counter.Name,
                                Delta = counter.Value
                            });
                        }

                        commands.Add(new CountersBatchCommandData(put.Value.Id, counterOperations)
                        {
                            FromEtl = true
                        });
                    }
                }
            }

            if (_counters != null)
            {
                foreach (var counter in _counters)
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
