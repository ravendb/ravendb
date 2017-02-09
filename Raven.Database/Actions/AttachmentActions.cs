// -----------------------------------------------------------------------
//  <copyright file="AttachmentActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    [Obsolete("Use RavenFS instead.")]
    public class AttachmentActions : ActionsBase
    {
        /// <summary>
        /// Requires to avoid having serialize writes to the same attachments
        /// </summary>
        private readonly ConcurrentDictionary<string, object> putAttachmentSerialLock = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        public AttachmentActions(DocumentDatabase database, IUuidGenerator uuidGenerator, ILog log)
            : base(database, uuidGenerator, log)
        {
        }

        public IEnumerable<AttachmentInformation> GetStaticsStartingWith(string idPrefix, int start, int pageSize)
        {
            if (idPrefix == null) throw new ArgumentNullException("idPrefix");
            IEnumerable<AttachmentInformation> attachments = null;
            TransactionalStorage.Batch(actions =>
            {
                attachments = actions.Attachments.GetAttachmentsStartingWith(idPrefix, start, pageSize)
                    .Select(information =>
                    {
                        var processAttachmentReadVetoes = ProcessAttachmentReadVetoes(information);
                        ExecuteAttachmentReadTriggers(processAttachmentReadVetoes);
                        return processAttachmentReadVetoes;
                    })
                    .Where(x => x != null)
                    .ToList();
            });
            return attachments;
        }

        public AttachmentInformation[] GetAttachments(int start, int pageSize, Etag etag, string startsWith, long maxSize)
        {
            AttachmentInformation[] attachments = null;

            TransactionalStorage.Batch(actions =>
            {
                if (string.IsNullOrEmpty(startsWith) == false)
                    attachments = actions.Attachments.GetAttachmentsStartingWith(startsWith, start, pageSize).ToArray();
                else if (etag != null)
                    attachments = actions.Attachments.GetAttachmentsAfter(etag, pageSize, maxSize).ToArray();
                else
                    attachments = actions.Attachments.GetAttachmentsByReverseUpdateOrder(start).Take(pageSize).ToArray();

            });
            return attachments;
        }

        public Attachment GetStatic(string name)
        {
            if (name == null)
                throw new ArgumentNullException("name");
            name = name.Trim();
            Attachment attachment = null;
            TransactionalStorage.Batch(actions =>
            {
                attachment = actions.Attachments.GetAttachment(name);

                attachment = ProcessAttachmentReadVetoes(name, attachment);

                ExecuteAttachmentReadTriggers(name, attachment);
            });
            return attachment;
        }

        public Etag PutStatic(string name, Etag etag, Stream data, RavenJObject metadata)
        {
            if (name == null)
                throw new ArgumentNullException("name");
            name = name.Trim();

            if (Encoding.Unicode.GetByteCount(name) >= 2048)
                throw new ArgumentException("The key must be a maximum of 2,048 bytes in Unicode, 1,024 characters", "name");

            var locker = putAttachmentSerialLock.GetOrAdd(name, s => new object());
            Monitor.Enter(locker);
            try
            {
                Etag newEtag = Etag.Empty;
                TransactionalStorage.Batch(actions =>
                {
                    AssertAttachmentPutOperationNotVetoed(name, metadata, data);

                    Database.AttachmentPutTriggers.Apply(trigger => trigger.OnPut(name, data, metadata));

                    newEtag = actions.Attachments.AddAttachment(name, etag, data, metadata);

                    Database.AttachmentPutTriggers.Apply(trigger => trigger.AfterPut(name, data, metadata, newEtag));

                    WorkContext.ShouldNotifyAboutWork(() => "PUT ATTACHMENT " + name);
                });
                TransactionalStorage
                    .ExecuteImmediatelyOrRegisterForSynchronization(() =>
                {
                    Database.AttachmentPutTriggers.Apply(trigger => trigger.AfterCommit(name, data, metadata, newEtag));
                    var newAttachmentNotification = new AttachmentChangeNotification()
                    {
                        Id = name,
                        Type = AttachmentChangeTypes.Put,
                        Etag = newEtag
                    };
                    Database.Notifications.RaiseNotifications(newAttachmentNotification, metadata);
                });
                return newEtag;
            }
            finally
            {
                Monitor.Exit(locker);
                putAttachmentSerialLock.TryRemove(name, out locker);
            }
        }

        public void DeleteStatic(string name, Etag etag)
        {
            if (name == null)
                throw new ArgumentNullException("name");
            name = name.Trim();
            TransactionalStorage.Batch(actions =>
            {
                AssertAttachmentDeleteOperationNotVetoed(name);

                Database.AttachmentDeleteTriggers.Apply(x => x.OnDelete(name));

                actions.Attachments.DeleteAttachment(name, etag);

                Database.AttachmentDeleteTriggers.Apply(x => x.AfterDelete(name));

                WorkContext.ShouldNotifyAboutWork(() => "DELETE ATTACHMENT " + name);
            });
            TransactionalStorage
                .ExecuteImmediatelyOrRegisterForSynchronization(
                    () =>
                    {
                        Database.AttachmentDeleteTriggers.Apply(trigger => trigger.AfterCommit(name));
                        var newAttachmentNotification = new AttachmentChangeNotification()
                        {
                            Id = name,
                            Type = AttachmentChangeTypes.Delete,
                            Etag = etag
                        };
                        Database.Notifications.RaiseNotifications(newAttachmentNotification, null);
                    });

        }

        private void AssertAttachmentPutOperationNotVetoed(string key, RavenJObject metadata, Stream data)
        {
            var vetoResult = Database.AttachmentPutTriggers
                .Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowPut(key, data, metadata) })
                .FirstOrDefault(x => x.VetoResult.IsAllowed == false);
            if (vetoResult != null)
            {
                throw new OperationVetoedException("PUT vetoed on attachment " + key + " by " + vetoResult.Trigger +
                                                   " because: " + vetoResult.VetoResult.Reason);
            }
        }

        private Attachment ProcessAttachmentReadVetoes(string name, Attachment attachment)
        {
            if (attachment == null)
                return null;

            var foundResult = false;
            foreach (var attachmentReadTriggerLazy in Database.AttachmentReadTriggers)
            {
                if (foundResult)
                    break;
                var attachmentReadTrigger = attachmentReadTriggerLazy.Value;
                var readVetoResult = attachmentReadTrigger.AllowRead(name, attachment.Data(), attachment.Metadata,
                                                                     ReadOperation.Load);
                switch (readVetoResult.Veto)
                {
                    case ReadVetoResult.ReadAllow.Allow:
                        break;
                    case ReadVetoResult.ReadAllow.Deny:
                        attachment.Data = () => new MemoryStream(new byte[0]);
                        attachment.Size = 0;
                        attachment.Metadata = new RavenJObject
                                                {
                                                    {
                                                        "Raven-Read-Veto",
                                                        new RavenJObject
                                                            {
                                                                {"Reason", readVetoResult.Reason},
                                                                {"Trigger", attachmentReadTrigger.ToString()}
                                                            }
                                                        }
                                                };
                        foundResult = true;
                        break;
                    case ReadVetoResult.ReadAllow.Ignore:
                        attachment = null;
                        foundResult = true;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(readVetoResult.Veto.ToString());
                }
            }
            return attachment;
        }

        private void ExecuteAttachmentReadTriggers(string name, Attachment attachment)
        {
            if (attachment == null)
                return;

            foreach (var attachmentReadTrigger in Database.AttachmentReadTriggers)
            {
                attachmentReadTrigger.Value.OnRead(name, attachment);
            }
        }

        private AttachmentInformation ProcessAttachmentReadVetoes(AttachmentInformation attachment)
        {
            if (attachment == null)
                return null;

            var foundResult = false;
            foreach (var attachmentReadTriggerLazy in Database.AttachmentReadTriggers)
            {
                if (foundResult)
                    break;
                var attachmentReadTrigger = attachmentReadTriggerLazy.Value;
                var readVetoResult = attachmentReadTrigger.AllowRead(attachment.Key, null, attachment.Metadata,
                                                                     ReadOperation.Load);
                switch (readVetoResult.Veto)
                {
                    case ReadVetoResult.ReadAllow.Allow:
                        break;
                    case ReadVetoResult.ReadAllow.Deny:
                        attachment.Size = 0;
                        attachment.Metadata = new RavenJObject
                                                {
                                                    {
                                                        "Raven-Read-Veto",
                                                        new RavenJObject
                                                            {
                                                                {"Reason", readVetoResult.Reason},
                                                                {"Trigger", attachmentReadTrigger.ToString()}
                                                            }
                                                        }
                                                };
                        foundResult = true;
                        break;
                    case ReadVetoResult.ReadAllow.Ignore:
                        attachment = null;
                        foundResult = true;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(readVetoResult.Veto.ToString());
                }
            }
            return attachment;
        }

        private void ExecuteAttachmentReadTriggers(AttachmentInformation information)
        {
            if (information == null)
                return;

            foreach (var attachmentReadTrigger in Database.AttachmentReadTriggers)
            {
                attachmentReadTrigger.Value.OnRead(information);
            }
        }

        private void AssertAttachmentDeleteOperationNotVetoed(string key)
        {
            var vetoResult = Database.AttachmentDeleteTriggers
                .Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowDelete(key) })
                .FirstOrDefault(x => x.VetoResult.IsAllowed == false);
            if (vetoResult != null)
            {
                throw new OperationVetoedException("DELETE vetoed on attachment " + key + " by " + vetoResult.Trigger +
                                                   " because: " + vetoResult.VetoResult.Reason);
            }
        }
    }
}
