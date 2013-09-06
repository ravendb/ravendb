namespace Raven.Database.Storage.Voron.StorageActions
{
	using Raven.Abstractions.Data;
	using Raven.Database.Data;
	using Raven.Database.Impl;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;

	using System;
	using System.Collections.Generic;
	using System.IO;

	using Raven.Abstractions.Exceptions;
	using Raven.Abstractions.Extensions;
	using Raven.Abstractions.Logging;

	using global::Voron.Impl;

	public class AttachmentsStorageActions : IAttachmentsStorageActions
	{
		private readonly Table attachmentsTable;

		private readonly WriteBatch writeBatch;
		private readonly SnapshotReader snapshot;
		private readonly IUuidGenerator uuidGenerator;
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		public AttachmentsStorageActions(Table attachmentsTable, WriteBatch writeBatch, SnapshotReader snapshot, IUuidGenerator uuidGenerator)
		{
			this.attachmentsTable = attachmentsTable;
			this.writeBatch = writeBatch;
			this.snapshot = snapshot;
			this.uuidGenerator = uuidGenerator;
		}

		public Etag AddAttachment(string key, Etag etag, Stream data, RavenJObject headers)
		{
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");
			if (data == null)
				throw new InvalidOperationException("When adding new attachment, the attachment data must be specified");

			var lowercaseKey = key.ToLowerInvariant();
			var dataKey = Util.DataKey(lowercaseKey);
			var metadataKey = Util.MetadataKey(lowercaseKey);

			var keyByETagIndice = this.attachmentsTable.GetIndex(Tables.Attachments.Indices.ByEtag);
			var isUpdate = this.attachmentsTable.Contains(this.snapshot, lowercaseKey);			
			if (isUpdate)
			{
				if (!this.attachmentsTable.Contains(this.snapshot, metadataKey)) //precaution
				{
					throw new ApplicationException(String.Format(@"Headers for attachment with key = '{0}' were not found, 
																		but the attachment itself was found. Data corruption?",key));
				}

				Etag existingEtag = null;
				if (etag != null && !this.IsAttachmentEtagMatch(metadataKey, etag, out existingEtag))
				{
					throw new ConcurrencyException("PUT attempted on attachment '" + key +
											"' using a non current etag")
					{
						ActualETag = existingEtag,
						ExpectedETag = etag
					};
				}

				keyByETagIndice.Delete(this.writeBatch, existingEtag.ToString());
			}

			var newETag = this.uuidGenerator.CreateSequentialUuid(UuidType.Attachments);
	
			data.Position = 0;
			this.attachmentsTable.Add(this.writeBatch, dataKey, data);
			keyByETagIndice.Add(this.writeBatch, newETag.ToString(), key);

			this.WriteAttachmentMetadata(metadataKey, newETag, headers);

			return newETag;
		}

		public void DeleteAttachment(string key, Etag etag)
		{
			throw new NotImplementedException();
		}

		public Attachment GetAttachment(string key)
		{
			var lowerKey = key.ToLowerInvariant();
			var dataKey = Util.DataKey(lowerKey);
			var metadataKey = Util.MetadataKey(lowerKey);
			if (!this.attachmentsTable.Contains(this.snapshot, dataKey))
				return null;

			using (var dataReadResult = this.attachmentsTable.Read(this.snapshot, dataKey))
			{
				if (dataReadResult == null) return null;
				Etag currentEtag;
				var headers = this.ReadAttachmentMetadata(metadataKey, out currentEtag);

				var attachmentStream = new MemoryStream((int)dataReadResult.Stream.Length);
				dataReadResult.Stream.CopyTo(attachmentStream);
				
				attachmentStream.Position = 0;
				var attachment = new Attachment()
				{
					Key = key,
					Etag = currentEtag,
					Metadata = headers,
					Data = () => attachmentStream,
					Size = (int)attachmentStream.Length					
				};

				return attachment;
			}
		}

		private Stream GetAttachmentStream(string dataKey)
		{
			var readResult = this.attachmentsTable.Read(this.snapshot, dataKey);
			if (readResult == null)
				return null;

			return readResult.Stream;
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsAfter(Etag value, int take, long maxTotalSize)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsStartingWith(string idPrefix, int start, int pageSize)
		{
			throw new NotImplementedException();
		}

		private Etag ReadCurrentEtag(string metadataKey)
		{
			using (var metadataReadResult = this.attachmentsTable.Read(this.snapshot, metadataKey))
			{
				if (metadataReadResult == null) //precaution
				{
					return null;
				}

				return metadataReadResult.Stream.ReadEtag();
			}
		}

		private RavenJObject ReadAttachmentMetadata(string metadataKey, out Etag etag)
		{
			using (var metadataReadResult = this.attachmentsTable.Read(this.snapshot, metadataKey))
			{
				if (metadataReadResult == null) //precaution
				{
					etag = null;
					return null;
				}

				etag = metadataReadResult.Stream.ReadEtag();
				var metadata = metadataReadResult.Stream.ToJObject();
				return metadata;
			}
		}

		private void WriteAttachmentMetadata(string metadataKey, Etag etag, RavenJObject headers)
		{
			var memoryStream = new MemoryStream();
			memoryStream.Write(etag);
			headers.WriteTo(memoryStream);

			memoryStream.Position = 0;
			this.attachmentsTable.Add(this.writeBatch, metadataKey, memoryStream);
		}

		private bool IsAttachmentEtagMatch(string metadataKey, Etag etag, out Etag existingEtag)
		{
			existingEtag = this.ReadCurrentEtag(metadataKey);
			
			if (existingEtag == null) return false;

			else if (etag == null) return true;

			else return existingEtag == etag;
		}
	}
}
