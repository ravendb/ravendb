using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Json.Linq;

namespace Raven.Database.Storage.RAM
{
	public class RamAttachmentsStorageActions : IAttachmentsStorageActions
	{
		private readonly RamState state;
		private readonly IUuidGenerator generator;

		public RamAttachmentsStorageActions(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
		}

		public Guid AddAttachment(string key, Guid? etag, Stream data, RavenJObject headers)
		{
			throw new NotImplementedException();
		}

		public void DeleteAttachment(string key, Guid? etag)
		{
			throw new NotImplementedException();
		}

		public Attachment GetAttachment(string key)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsAfter(Guid value, int take, long maxTotalSize)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsStartingWith(string idPrefix, int start, int pageSize)
		{
			throw new NotImplementedException();
		}
	}
}