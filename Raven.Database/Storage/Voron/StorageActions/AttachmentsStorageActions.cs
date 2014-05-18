using Raven.Abstractions.Util.Streams;
using Raven.Database.Util.Streams;

using Voron;
using Voron.Impl;

namespace Raven.Database.Storage.Voron.StorageActions
{
	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Abstractions.Extensions;
	using Raven.Abstractions.Logging;
    using Raven.Abstractions.Util;
	using Raven.Database.Data;
	using Raven.Database.Impl;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;
	using System;
	using System.Collections.Generic;
	using System.IO;

    public class AttachmentsStorageActions : StorageActionsBase, IAttachmentsStorageActions
	{
		private readonly Table attachmentsTable;

		private readonly Reference<WriteBatch> writeBatch;
		private readonly IUuidGenerator uuidGenerator;

	    private readonly TableStorage tableStorage;

        private readonly Index metadataIndex;

	    private readonly Raven.Storage.Voron.TransactionalStorage transactionalStorage;

		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		public AttachmentsStorageActions(Table attachmentsTable,
										 Reference<WriteBatch> writeBatch, 
										 Reference<SnapshotReader> snapshot, 
									     IUuidGenerator uuidGenerator, 
                                         TableStorage tableStorage,
                                         Raven.Storage.Voron.TransactionalStorage transactionalStorage, 
                                         IBufferPool bufferPool)
            :base(snapshot, bufferPool)
		{
			this.attachmentsTable = attachmentsTable;
			this.writeBatch = writeBatch;
			this.uuidGenerator = uuidGenerator;
		    this.tableStorage = tableStorage;
		    this.transactionalStorage = transactionalStorage;

            metadataIndex = tableStorage.Attachments.GetIndex(Tables.Attachments.Indices.Metadata);
		}

		public Etag AddAttachment(string key, Etag etag, Stream data, RavenJObject headers)
		{
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

            var loweredKey = CreateKey(key);

			var keyByETagIndice = attachmentsTable.GetIndex(Tables.Attachments.Indices.ByEtag);
            var isUpdate = attachmentsTable.Contains(Snapshot, loweredKey, writeBatch.Value);
			if (isUpdate)
			{
				if (!metadataIndex.Contains(Snapshot, loweredKey, writeBatch.Value)) //precaution
				{
					throw new ApplicationException(String.Format(@"Headers for attachment with key = '{0}' were not found,
but the attachment itself was found. Data corruption?", key));
				}

				Etag existingEtag = null;
				if (etag != null && !IsAttachmentEtagMatch(loweredKey, etag, out existingEtag))
				{
					throw new ConcurrencyException("PUT attempted on attachment '" + key +
					"' using a non current etag")
					{
						ActualETag = existingEtag,
						ExpectedETag = etag
					};
				}

				if (existingEtag != null) //existingEtag can be null if etag parameter is null
					keyByETagIndice.Delete(writeBatch.Value, existingEtag.ToString());
				else
				{
					var currentEtag = ReadCurrentEtag(loweredKey);
					keyByETagIndice.Delete(writeBatch.Value, currentEtag.ToString());
				}
			}
			else
			{
				if (data == null)
					throw new InvalidOperationException("When adding new attachment, the attachment data must be specified");

				if (!data.CanRead) //precaution
					throw new InvalidOperationException("When adding/updating attachment, the attachment data stream must be readable");
			}

			var newETag = uuidGenerator.CreateSequentialUuid(UuidType.Attachments);

			if (data != null)
			{
				if (data.CanSeek)
				{
					data.Seek(0, SeekOrigin.Begin);
					attachmentsTable.Add(writeBatch.Value, loweredKey, data);
				}
				else //handle streams like GzipStream
				{
					try
					{
					    var tempStream = CreateStream();
						data.CopyTo(tempStream);
						tempStream.Seek(0, SeekOrigin.Begin);
						attachmentsTable.Add(writeBatch.Value, loweredKey, tempStream);
					}
					finally
					{
						data.Dispose();
					}
				}
			}

			keyByETagIndice.Add(writeBatch.Value, newETag.ToString(), key);

			WriteAttachmentMetadata(loweredKey, newETag, headers);

			if (data != null && data.CanSeek)
				logger.Debug("Fetched document attachment (key = '{0}', attachment size = {1})", key, data.Length);
			else
				logger.Debug("Fetched document attachment (key = '{0}')", key);

			return newETag;
		}

		public void DeleteAttachment(string key, Etag etag)
		{
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

            var loweredKey = CreateKey(key);

            if (!attachmentsTable.Contains(Snapshot, loweredKey, writeBatch.Value))
			{
				logger.Debug("Attachment with key '{0}' was not found, and considered deleted", key);
				return;
			}

            var existingEtag = ReadCurrentEtag(loweredKey);
			if (existingEtag == null) //precaution --> should never be null at this stage
				throw new InvalidDataException("The attachment exists, but failed reading etag from metadata. Data corruption?");

			if (existingEtag != etag && etag != null)
			{
				throw new ConcurrencyException("DELETE attempted on attachment '" + key + "' using a non current etag")
				{
					ActualETag = existingEtag,
					ExpectedETag = etag
				};
			}

            attachmentsTable.Delete(writeBatch.Value, loweredKey);
            metadataIndex.Delete(writeBatch.Value, loweredKey);
			attachmentsTable.GetIndex(Tables.Attachments.Indices.ByEtag)
							.Delete(writeBatch.Value, existingEtag);

			logger.Debug("Deleted document attachment (key = '{0}')", key);
		}

		public Attachment GetAttachment(string key)
		{
            var loweredKey = CreateKey(key);

            if (!attachmentsTable.Contains(Snapshot, loweredKey, writeBatch.Value))
				return null;

            var dataReadResult = attachmentsTable.Read(Snapshot, loweredKey, writeBatch.Value);
			if (dataReadResult == null) return null;

			Etag currentEtag;
            var headers = ReadAttachmentMetadata(loweredKey, out currentEtag);
			if (headers == null) //precaution --> should never be null at this stage
				throw new InvalidDataException("The attachment exists, but failed reading metadata. Data corruption?");

			using (var stream = dataReadResult.Reader.AsStream())
			{
				var attachment = new Attachment
				{
					Key = key,
					Etag = currentEtag,
					Metadata = headers,
					Data = () =>
					{
						var storageActions = transactionalStorage.GetCurrentBatch();
						var attachmentStorageActions = storageActions.Attachments as AttachmentsStorageActions;
						if (attachmentStorageActions == null)
							throw new InvalidOperationException("Something is very wrong here. Storage actions define invalid attachment storage actions object");

                        var attachmentDataStream = attachmentStorageActions.GetAttachmentStream(loweredKey);
						return attachmentDataStream;
					},
					Size = (int) stream.Length
				};

				logger.Debug("Fetched document attachment (key = '{0}', attachment size = {1})", key, stream.Length);
				return attachment;
			}
		}

	    public long GetAttachmentsCount()
	    {
            return tableStorage.GetEntriesCount(tableStorage.Attachments);
	    }

	    internal Stream GetAttachmentStream(Slice key)
		{
            if (!attachmentsTable.Contains(Snapshot, key, writeBatch.Value))
				return new MemoryStream();

            var dataReadResult = attachmentsTable.Read(Snapshot, key, writeBatch.Value);
			return dataReadResult.Reader.AsStream();
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start)
		{
			if (start < 0)
				throw new ArgumentException("must have zero or positive value", "start");

			using (var iter = attachmentsTable.GetIndex(Tables.Attachments.Indices.ByEtag)
											  .Iterate(Snapshot, writeBatch.Value))
			{
				if (!iter.Seek(Slice.AfterAllKeys))
					yield break;

				if (!iter.Skip(-start))
					yield break;
				do
				{
					if (iter.CurrentKey == null || iter.CurrentKey.Equals(Slice.Empty))
						yield break;

					string key;
					using (var keyStream = iter.CreateReaderForCurrent().AsStream())
						key = keyStream.ReadStringWithoutPrefix();

					var attachmentInfo = AttachmentInfoByKey(key);
					if (attachmentInfo == null)
					{
						throw new ApplicationException(String.Format("Possible data corruption - the key = '{0}' was found in the attachments indice, but matching attachment data was not found", key));
					}

					yield return attachmentInfo;
				} while (iter.MovePrev());
			}
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsAfter(Etag value, int take, long maxTotalSize)
		{
			if (take < 0)
				throw new ArgumentException("must have zero or positive value", "take");
			if (maxTotalSize < 0)
				throw new ArgumentException("must have zero or positive value", "maxTotalSize");

			if (take == 0) yield break; //edge case

			using (var iter = attachmentsTable.GetIndex(Tables.Attachments.Indices.ByEtag)
											  .Iterate(Snapshot, writeBatch.Value))
			{
				if (!iter.Seek(Slice.BeforeAllKeys))
					yield break;

				var fetchedDocumentCount = 0;
				var fetchedTotalSize = 0;
				do
				{
					if (iter.CurrentKey == null || iter.CurrentKey.Equals(Slice.Empty))
						yield break;

					var attachmentEtag = Etag.Parse(iter.CurrentKey.ToString());

					if (!EtagUtil.IsGreaterThan(attachmentEtag, value)) continue;

					string key;
					using (var keyStream = iter.CreateReaderForCurrent().AsStream())
						key = keyStream.ReadStringWithoutPrefix();

					var attachmentInfo = AttachmentInfoByKey(key);

					fetchedTotalSize += attachmentInfo.Size;
					fetchedDocumentCount++;

					if (fetchedTotalSize > maxTotalSize)
						yield break;

					if (fetchedDocumentCount >= take || fetchedTotalSize == maxTotalSize)
					{
						yield return attachmentInfo;
						yield break;
					}

					yield return attachmentInfo;

				} while (iter.MoveNext());
			}

		}

		public IEnumerable<AttachmentInformation> GetAttachmentsStartingWith(string idPrefix, int start, int pageSize)
		{
			if (String.IsNullOrEmpty(idPrefix))
				throw new ArgumentNullException("idPrefix");
			if (start < 0)
				throw new ArgumentException("must have zero or positive value", "start");
			if (pageSize < 0)
				throw new ArgumentException("must have zero or positive value", "pageSize");

			if (pageSize == 0) //edge case
				yield break;

			using (var iter = attachmentsTable.Iterate(Snapshot, writeBatch.Value))
			{
				iter.RequiredPrefix = idPrefix.ToLowerInvariant();
				if (iter.Seek(iter.RequiredPrefix) == false)
					yield break;

				var fetchedDocumentCount = 0;
				var alreadySkippedCount = 0; //we have to do it this way since we store in the same tree both data and metadata entries
				do
				{
					var key = iter.CurrentKey.ToString();

					if (start > 0 && alreadySkippedCount++ < start) continue;

					fetchedDocumentCount++;
                    yield return AttachmentInfoByKey(key);
				} while (iter.MoveNext() && fetchedDocumentCount < pageSize);
			}
		}

		private AttachmentInformation AttachmentInfoByKey(string key)
		{
			var attachment = GetAttachment(key);

			if (attachment == null) //precaution
				throw new InvalidDataException(
					"Tried to read attachment with key='{0}' but failed. Data mismatch between attachment indice and attachment data? (key by etag indice)");

			var attachmentInfo = new AttachmentInformation
			{
				Etag = attachment.Etag,
				Key = attachment.Key,
				Metadata = attachment.Metadata,
				Size = attachment.Size
			};
			return attachmentInfo;
		}

		private Etag ReadCurrentEtag(Slice key)
		{
            var metadataReadResult = metadataIndex.Read(Snapshot, key, writeBatch.Value);
			if (metadataReadResult == null)
				return null;
			using (var stream = metadataReadResult.Reader.AsStream())
			{
				return stream.ReadEtag();
			}
		}

        private RavenJObject ReadAttachmentMetadata(Slice key, out Etag etag)
		{
            var metadataReadResult = metadataIndex.Read(Snapshot, key, writeBatch.Value);
			if (metadataReadResult == null) //precaution
			{
				etag = null;
				return null;
			}

			using (var stream = metadataReadResult.Reader.AsStream())
			{
				etag = stream.ReadEtag();
				var metadata = stream.ToJObject();
				return metadata;
			}
		}

        private void WriteAttachmentMetadata(Slice key, Etag etag, RavenJObject headers)
		{
            var memoryStream = CreateStream();
			memoryStream.Write(etag);
			headers.WriteTo(memoryStream);

			memoryStream.Position = 0;
            metadataIndex.Add(writeBatch.Value, key, memoryStream);
		}

        private bool IsAttachmentEtagMatch(Slice key, Etag etag, out Etag existingEtag)
		{
            existingEtag = ReadCurrentEtag(key);

			if (existingEtag == null) 
                return false;

            if (etag == null) 
                return true;

            return existingEtag == etag;
		}
	}
}
