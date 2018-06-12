using System.Collections.Generic;
using System.Diagnostics;
using Jint.Native;
using Raven.Client.Documents.Commands.Batches;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlScriptRun
    {
        private readonly List<ICommandData> _deletes = new List<ICommandData>();

        private Dictionary<JsValue, (string Id, BlittableJsonReaderObject Document)> _puts;
        
        private Dictionary<JsValue, List<(string Name, Attachment Attachment)>> _addAttachments;

        private Dictionary<JsValue, Attachment> _loadedAttachments;

        private List<ICommandData> _putsAndAttachments;

        public void Delete(ICommandData command)
        {
            Debug.Assert(command is DeleteCommandData || command is DeletePrefixedCommandData);

            _deletes.Add(command);
        }

        public void PutDocumentAndAttachmentsIfAny(string id, BlittableJsonReaderObject doc, List<Attachment> attachments)
        {
            if (_putsAndAttachments == null)
                _putsAndAttachments = new List<ICommandData>();

            _putsAndAttachments.Add(new PutCommandDataWithBlittableJson(id, null, doc));

            if (attachments != null && attachments.Count > 0)
            {
                foreach (var attachment in attachments)
                {
                    _putsAndAttachments.Add(new PutAttachmentCommandData(id, attachment.Name, attachment.Stream, attachment.ContentType, null));
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

        public List<ICommandData> GetCommands()
        {
            // let's send deletions first
            var commands = _deletes;

            if (_putsAndAttachments != null)
            {
                foreach (var command in _putsAndAttachments)
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
                }
            }
            
            return commands;
        }
    }
}
