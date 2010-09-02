using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;

namespace Raven.Database.Storage.StorageActions
{
	public interface IAttachmentsStorageActions
	{
		void AddAttachment(string key, Guid? etag, byte[] data, JObject headers);
		void DeleteAttachment(string key, Guid? etag);
		Attachment GetAttachment(string key);
		IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start);
		IEnumerable<AttachmentInformation> GetAttachmentsAfter(Guid value);
	}
}