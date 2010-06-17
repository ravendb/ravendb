using System;
using Raven.Database.Data;

namespace Raven.Database.Storage.StorageActions
{
	public interface IAttachmentsStorageActions
	{
		void AddAttachment(string key, Guid? etag, byte[] data, string headers);
		void DeleteAttachment(string key, Guid? etag);
		Attachment GetAttachment(string key);
	}
}