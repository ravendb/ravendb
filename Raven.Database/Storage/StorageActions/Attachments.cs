using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Exceptions;

namespace Raven.Database.Storage.StorageActions
{
	public partial class DocumentStorageActions 
	{
		public void AddAttachment(string key, Guid? etag, byte[] data, string headers)
		{
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var isUpdate = Api.TrySeek(session, Files, SeekGrbit.SeekEQ);
			if (isUpdate)
			{
				var existingEtag = new Guid(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]));
				if (existingEtag != etag && etag != null)
				{
					throw new ConcurrencyException("PUT attempted on attachment '" + key +
						"' using a non current etag")
					{
						ActualETag = etag.Value,
						ExpectedETag = existingEtag
					};
				}
			}
			else
			{
				if (Api.TryMoveFirst(session, Details))
					Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["attachment_count"], 1);
			}

			Guid newETag;
			DocumentDatabase.UuidCreateSequential(out newETag);
			using (var update = new Update(session, Files, isUpdate ? JET_prep.Replace : JET_prep.Insert))
			{
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["name"], key, Encoding.Unicode);
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["data"], data);
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["etag"], newETag.ToByteArray());
				Api.SetColumn(session, Files, tableColumnsCache.FilesColumns["metadata"], headers, Encoding.Unicode);

				update.Save();
			}
			logger.DebugFormat("Adding attachment {0}", key);
		}

		public void DeleteAttachment(string key, Guid? etag)
		{
			if (Api.TryMoveFirst(session, Details))
				Api.EscrowUpdate(session, Details, tableColumnsCache.DetailsColumns["attachment_count"], -1);
			Api.JetSetCurrentIndex(session, Files, "by_name");
			Api.MakeKey(session, Files, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, Files, SeekGrbit.SeekEQ) == false)
			{
				logger.DebugFormat("Attachment with key '{0}' was not found, and considered deleted", key);
				return;
			}
			var fileEtag = new Guid(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"]));
			if (fileEtag != etag && etag != null)
			{
				throw new ConcurrencyException("DELETE attempted on attachment '" + key +
					"' using a non current etag")
				{
					ActualETag = etag.Value,
					ExpectedETag = fileEtag
				};
			}

			Api.JetDelete(session, Files);
			logger.DebugFormat("Attachment with key '{0}' was deleted", key);
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
				Data = Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["data"]),
				Etag = new Guid(Api.RetrieveColumn(session, Files, tableColumnsCache.FilesColumns["etag"])),
				Metadata = JObject.Parse(metadata)
			};
		}
	}
}