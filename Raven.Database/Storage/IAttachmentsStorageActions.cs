using System;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;

namespace Raven.Database.Storage.StorageActions
{
	public interface IAttachmentsStorageActions
	{
		void AddAttachment(string key, Guid? etag, byte[] data, JObject headers);
		void DeleteAttachment(string key, Guid? etag);
		Attachment GetAttachment(string key);
	}
}