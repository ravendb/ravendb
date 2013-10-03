using System.Diagnostics;
using System.Text;
using System.Web.UI;
using Raven.Abstractions.Util;
using Voron;

namespace Raven.Database.Storage.Voron.StorageActions
{
	using global::Voron.Impl;
	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Abstractions.Extensions;
	using Raven.Abstractions.Logging;
	using Raven.Database.Data;
	using Raven.Database.Impl;
	using Raven.Database.Storage.Voron.Impl;
	using Raven.Json.Linq;
	using System;
	using System.Collections.Generic;
	using System.IO;

	public class AttachmentsStorageActions : IAttachmentsStorageActions
	{
		private readonly Table attachmentsTable;

		private readonly WriteBatch writeBatch;
		private readonly SnapshotReader snapshot;
		private readonly IUuidGenerator uuidGenerator;
		private readonly Raven.Storage.Voron.TransactionalStorage transactionalStorage;

		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		public AttachmentsStorageActions(Table attachmentsTable, 
										 WriteBatch writeBatch, 
										 SnapshotReader snapshot, 
									     IUuidGenerator uuidGenerator, 
										 Raven.Storage.Voron.TransactionalStorage transactionalStorage)
		{
			this.attachmentsTable = attachmentsTable;
			this.writeBatch = writeBatch;
			this.snapshot = snapshot;
			this.uuidGenerator = uuidGenerator;
			this.transactionalStorage = transactionalStorage;
		}

		public Etag AddAttachment(string key, Etag etag, Stream data, RavenJObject headers)
		{
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			var lowercaseKey = key.ToLowerInvariant();
			var dataKey = Util.DataKey(lowercaseKey);
			var metadataKey = Util.MetadataKey(lowercaseKey);

			var keyByETagIndice = attachmentsTable.GetIndex(Tables.Attachments.Indices.ByEtag);
			var isUpdate = attachmentsTable.Contains(snapshot, dataKey, writeBatch);
			if (isUpdate)
			{
				if (!attachmentsTable.Contains(snapshot, metadataKey, writeBatch)) //precaution
				{
					throw new ApplicationException(String.Format(@"Headers for attachment with key = '{0}' were not found,
but the attachment itself was found. Data corruption?", key));
				}

				Etag existingEtag = null;
				if (etag != null && !IsAttachmentEtagMatch(metadataKey, etag, out existingEtag))
				{
					throw new ConcurrencyException("PUT attempted on attachment '" + key +
					"' using a non current etag")
					{
						ActualETag = existingEtag,
						ExpectedETag = etag
					};
				}

				if (existingEtag != null) //existingEtag can be null if etag parameter is null
					keyByETagIndice.Delete(writeBatch, existingEtag.ToString());
				else
				{
					var currentEtag = ReadCurrentEtag(metadataKey);
					keyByETagIndice.Delete(writeBatch, currentEtag.ToString());
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
					attachmentsTable.Add(writeBatch, dataKey, data);
				}
				else //handle streams like GzipStream
				{
					try
					{
						var tempStream = new MemoryStream();
						data.CopyTo(tempStream);
						tempStream.Seek(0, SeekOrigin.Begin);
						attachmentsTable.Add(writeBatch, dataKey, tempStream);
					}
					finally
					{
						data.Dispose();
					}
				}
			}

			keyByETagIndice.Add(writeBatch, newETag.ToString(), key);

			WriteAttachmentMetadata(metadataKey, newETag, headers);

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

			var lowerKey = key.ToLowerInvariant();
			var dataKey = Util.DataKey(lowerKey);
			var metadataKey = Util.MetadataKey(lowerKey);

			if (!attachmentsTable.Contains(snapshot, dataKey, writeBatch))
			{
				logger.Debug("Attachment with key '{0}' was not found, and considered deleted", key);
				return;
			}

			var existingEtag = ReadCurrentEtag(metadataKey);
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

			attachmentsTable.Delete(writeBatch, dataKey);
			attachmentsTable.Delete(writeBatch, metadataKey);
			attachmentsTable.GetIndex(Tables.Attachments.Indices.ByEtag)
							.Delete(writeBatch, existingEtag);

			logger.Debug("Deleted document attachment (key = '{0}')", key);
		}

		public Attachment GetAttachment(string key)
		{
			var lowerKey = key.ToLowerInvariant();
			var dataKey = Util.DataKey(lowerKey);
			var metadataKey = Util.MetadataKey(lowerKey);
			if (!attachmentsTable.Contains(snapshot, dataKey, writeBatch))
				return null;

			using (var dataReadResult = attachmentsTable.Read(snapshot, dataKey, writeBatch))
			{
				if (dataReadResult == null) return null;
				Etag currentEtag;
				var headers = ReadAttachmentMetadata(metadataKey, out currentEtag);
				if (headers == null) //precaution --> should never be null at this stage
					throw new InvalidDataException("The attachment exists, but failed reading metadata. Data corruption?");				

				var attachment = new Attachment()
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

						var attachmentDataStream = attachmentStorageActions.GetAttachmentStream(dataKey);
												
						return attachmentDataStream;
					},
					Size = (int)dataReadResult.Stream.Length
				};

				logger.Debug("Fetched document attachment (key = '{0}', attachment size = {1})", key, dataReadResult.Stream.Length);
				return attachment;
			}
		}

		internal Stream GetAttachmentStream(string dataKey)
		{
			if (!attachmentsTable.Contains(snapshot, dataKey, writeBatch))
				return new MemoryStream();

			var dataReadResult = attachmentsTable.Read(snapshot, dataKey, writeBatch);
			return dataReadResult.Stream;
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start)
		{
			if (start < 0)
				throw new ArgumentException("must have zero or positive value", "start");

			using (var iter = attachmentsTable.GetIndex(Tables.Attachments.Indices.ByEtag)
											  .Iterate(snapshot, writeBatch))
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
					using (var keyStream = iter.CreateStreamForCurrent())
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
											  .Iterate(snapshot, writeBatch))
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
					using (var keyStream = iter.CreateStreamForCurrent())
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

			using (var iter = attachmentsTable.Iterate(snapshot, writeBatch))
			{
				iter.RequiredPrefix = idPrefix.ToLowerInvariant();
				if (iter.Seek(iter.RequiredPrefix) == false)
					yield break;

				var fetchedDocumentCount = 0;
				var alreadySkippedCount = 0; //we have to do it this way since we store in the same tree both data and metadata entries
				do
				{
					var dataKey = iter.CurrentKey.ToString();
					if (dataKey.Contains(Util.MetadataSuffix)) continue;
					if (start > 0 && alreadySkippedCount++ < start) continue;

					fetchedDocumentCount++;
					yield return AttachmentInfoByKey(Util.OriginalKey(dataKey));
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

		private Etag ReadCurrentEtag(string metadataKey)
		{
			using (var metadataReadResult = attachmentsTable.Read(snapshot, metadataKey, writeBatch))
				return metadataReadResult == null ? null : metadataReadResult.Stream.ReadEtag();
		}

		private RavenJObject ReadAttachmentMetadata(string metadataKey, out Etag etag)
		{
			using (var metadataReadResult = attachmentsTable.Read(snapshot, metadataKey, writeBatch))
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
			attachmentsTable.Add(writeBatch, metadataKey, memoryStream);
		}

		private bool IsAttachmentEtagMatch(string metadataKey, Etag etag, out Etag existingEtag)
		{
			existingEtag = ReadCurrentEtag(metadataKey);

			if (existingEtag == null) return false;

			else if (etag == null) return true;

			else return existingEtag == etag;
		}
	}
}
