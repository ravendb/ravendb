using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Json;
using Raven.Database.Storage.StorageActions;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
    public class AttachmentsStorageActions : IAttachmentsStorageActions
    {
        private readonly TableStorage storage;
        private readonly ILog logger = LogManager.GetLogger(typeof (AttachmentsStorageActions));

        public AttachmentsStorageActions(TableStorage storage)
        {
            this.storage = storage;
        }

        public void AddAttachment(string key, Guid? etag, byte[] data, JObject headers)
        {
            AssertValidEtag(key, etag, "PUT");

            var ms = new MemoryStream();
            headers.WriteTo(ms);
            ms.Write(data,0,data.Length);
            var newEtag = DocumentDatabase.CreateSequentialUuid();
           var result = storage.Attachments.Put(new JObject
            {
                {"key", key},
                {"etag", newEtag.ToByteArray()}
            }, ms.ToArray());
           if (result == false)
               throw new ConcurrencyException("PUT attempted on attachment '" + key + "' while it was locked by another transaction");
            logger.DebugFormat("Adding attachment {0}", key);
        }

        private void AssertValidEtag(string key, Guid? etag, string op)
        {
            var readResult = storage.Attachments.Read(new JObject
            {
                {"key", key},
            });

            if(readResult != null && etag != null)
            {
                var existingEtag = new Guid(readResult.Key.Value<byte[]>("etag"));
                if (existingEtag != etag)
                {
                    throw new ConcurrencyException(op + " attempted on attachment '" + key +
                                                   "' using a non current etag")
                    {
                        ActualETag = etag.Value,
                        ExpectedETag = existingEtag
                    };
                }
            }
        }

        public void DeleteAttachment(string key, Guid? etag)
        {
            AssertValidEtag(key, etag, "DELETE");

            if (storage.Attachments.Remove(new JObject { { "key", key } }) == false)
                throw new ConcurrencyException("DELETE attempted on attachment '" + key +
                                               "'  while it was locked by another transaction");
            logger.DebugFormat("Attachment with key '{0}' was deleted", key);
        }

        public Attachment GetAttachment(string key)
        {
            var readResult = storage.Attachments.Read(new JObject { { "key", key } });
            if (readResult == null)
                return null;
            var attachmentDAta = readResult.Data();
            var memoryStream = new MemoryStream(attachmentDAta);
            var metadata = memoryStream.ToJObject();
            var data = new byte[readResult.Size - memoryStream.Position];
            Buffer.BlockCopy(attachmentDAta,(int)memoryStream.Position, data, 0, data.Length);
            return new Attachment
            {
                Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
                Metadata = metadata,
                Data = data
            };
        }

        public IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start)
        {
            return from key in storage.Attachments["ByEtag"].SkipFromEnd(start)
                   let attachment = GetAttachment(key.Value<string>("key"))
                   select new AttachmentInformation
                   {
                       Key = key.Value<string>("key"),
                       Etag = new Guid(key.Value<byte[]>("etag")),
                       Metadata = attachment.Metadata,
                       Size = attachment.Data.Length
                   };
        }

        public IEnumerable<AttachmentInformation> GetAttachmentsAfter(Guid value)
        {
            return from key in storage.Attachments["ByEtag"].SkipAfter(new JObject { { "etag", value.ToByteArray() } })
                   let attachment = GetAttachment(key.Value<string>("key"))
                   select new AttachmentInformation
                   {
                       Key = key.Value<string>("key"),
                       Etag = new Guid(key.Value<byte[]>("etag")),
                       Metadata = attachment.Metadata,
                       Size = attachment.Data.Length
                   };
        }
    }
}