//-----------------------------------------------------------------------
// <copyright file="Attachments.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Storage.Esent.StorageActions
{
	public partial class DocumentStorageActions : IAttachmentsStorageActions
	{
		public Guid AddAttachment(string key, Guid? etag, Stream data, RavenJObject headers)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdate = Api.TrySeek(session, Files, SeekGrbit.SeekEQ);
			if (isUpdate)
			{
				var existingEtag = Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]).TransfromToGuidWithProperSorting();
				if (existingEtag != etag && etag != null)
				{
					throw new ConcurrencyException("PUT attempted on attachment '" + key +
						"' using a non current etag")
					{
						ActualETag = existingEtag,
						ExpectedETag = etag.Value
					};
				}
			}
			else
			{
				if (Api.TryMoveFirst(session, Details))
					Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["attachment_count"], 1);
			}

			Guid newETag = uuidGenerator.CreateSequentialUuid();
			using (var update = new Update(session, Files, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["name"], key, Encoding.Unicode);
				using (var stream = new BufferedStream(new ColumnStream(session, Files, tableColumnsCache.FilesColumns["data"])))
				{
					data.CopyTo(stream);
					stream.Flush();
				}
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["etag"], newETag.TransformToValueForEsentSorting());
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["metadata"], headers.ToString(Formatting.None), Encoding.Unicode);

				update.Save();
			}
			logger.Debug("Adding attachment {0}", key);

		    return newETag;
		}

		public void DeleteAttachment(string key, Guid? etag)
		{
			if (Api.TryMoveFirst(session, Details))
				Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["attachment_count"], -1);
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
			{
				logger.Debug("Attachment with key '{0}' was not found, and considered deleted", key);
				return;
			}
			var fileEtag = Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]).TransfromToGuidWithProperSorting();
			if (fileEtag != etag && etag != null)
			{
				throw new ConcurrencyException("DELETE attempted on attachment '" + key +
					"' using a non current etag")
				{
					ActualETag = fileEtag,
					ExpectedETag = etag.Value
				};
			}

			Api.JetDelete(session, Files);
			logger.Debug("Attachment with key '{0}' was deleted", key);
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start)
		{
			Api.JetSetCurrentIndex(session, Files, "by_etag");
			Api.MoveAfterLast(session, Files);
			for (int i = 0; i < start; i++)
			{
				if (Api.TryMovePrevious(session, Files) == false)
					yield break;
			}
			while (Api.TryMovePrevious(session, Files))
			{
				yield return new AttachmentInformation
				{
					Size =  Api.RetrieveColumnSize(session, Files, tableColumnsCache.FilesColumns["data"]).Value,
					Etag = Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]).TransfromToGuidWithProperSorting(),
					Key = Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["name"], Encoding.Unicode),
					Metadata = RavenJObject.Parse(Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["metadata"], Encoding.Unicode))
				};
			}
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsAfter(Guid etag)
		{
			Api.JetSetCurrentIndex(session, Files, "by_etag");
			Api.MakeKey(session, Files, etag.TransformToValueForEsentSorting(), MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekGT) == false)
				yield break;
			do
			{
				var columnSize = Api.RetrieveColumnSize(session, Files, tableColumnsCache.FilesColumns["data"]);
				yield return new AttachmentInformation
				{
					Size = columnSize ?? 0,
					Etag = Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]).TransfromToGuidWithProperSorting(),
					Key = Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["name"], Encoding.Unicode),
					Metadata = RavenJObject.Parse(Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["metadata"], Encoding.Unicode))
				};
			} while (Api.TryMoveNext(session, Files));
		}

		public Attachment GetAttachment(string key)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
			{
				return null;
			}

			var metadata = Api.RetrieveColumnAsString(session, Files, tableColumnsCache.FilesColumns["metadata"], Encoding.Unicode);
			return new Attachment
			{
				Data = () =>
				{
					StorageActionsAccessor storageActionsAccessor = transactionalStorage.GetCurrentBatch();
					var documentStorageActions = ((DocumentStorageActions)storageActionsAccessor.Attachments);
					return documentStorageActions.GetAttachmentStream(key);
				}, 
				Size = (int)GetAttachmentStream(key).Length,
				Etag = Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]).TransfromToGuidWithProperSorting(),
				Metadata = RavenJObject.Parse(metadata)
			};
		}

		private Stream GetAttachmentStream(string key)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
			{
				throw new InvalidOperationException("Could not find attachment named: " + key);
			}

			return new BufferedStream(new ColumnStream(session, Files, tableColumnsCache.FilesColumns["data"]));
		}
	}
}
