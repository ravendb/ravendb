using System;
using log4net;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Storage.StorageActions;

namespace Raven.Storage.Managed
{
	public class AttachmentsStorageActions : AbstractStorageActions, IAttachmentsStorageActions
	{
		private static readonly ILog logger = LogManager.GetLogger(typeof(AttachmentsStorageActions));

		public void AddAttachment(string key, Guid? etag, byte[] data, JObject headers)
		{
			var attachmentPosition = Mutator.Attachments.FindValue(key);
			headers["@etag"] = new JValue(DocumentDatabase.CreateSequentialUuid().ToString());
			if (attachmentPosition == null)
			{
				Mutator.IncrementAttachmentCount();
			}
			else 
			{
				EnsureValidEtag(key, attachmentPosition.Value, etag);
			}

			var position = Writer.Position;
			headers.WriteTo(new BsonWriter(Writer));
			BinaryWriter.Write7BitEncodedInt(data.Length);
			Writer.Write(data, 0, data.Length);
			Mutator.Attachments.Add(key, position);
			logger.DebugFormat("Adding attachment {0}", key);
		}

		private void EnsureValidEtag(string key, long attachmentPosition, Guid? etag)
		{
			if (etag == null)
				return;
			Reader.Position = attachmentPosition;
			var storedHeaders = JObject.Load(new BsonReader(Reader));
			var existingEtag = new Guid(storedHeaders.Value<string>("@etag"));
			if (existingEtag != etag.Value)
			{
				throw new ConcurrencyException("PUT attempted on attachment '" + key +
					"' using a non current etag")
				{
					ActualETag = etag.Value,
					ExpectedETag = existingEtag
				};
			}
		}

		public void DeleteAttachment(string key, Guid? etag)
		{
			var attachmentPosition = Mutator.Attachments.FindValue(key);
			if (attachmentPosition == null)
				return;
			Mutator.DecrementAttachmentCount();
			EnsureValidEtag(key, attachmentPosition.Value, etag);

			Mutator.Attachments.Remove(key);
		}

		public Attachment GetAttachment(string key)
		{
			var attachmentPosition = Viewer.Attachments.FindValue(key);
			if (attachmentPosition == null)
				return null;
			Reader.Position = attachmentPosition.Value;
			var metadata = JObject.Load(new BsonReader(Reader));
			var size = BinaryReader.Read7BitEncodedInt();
			return new Attachment
			{
				Metadata = metadata,
				Etag = new Guid(metadata.Value<string>("@etag")),
				Data = BinaryReader.ReadBytes(size)
			};
		}
	}
}