//-----------------------------------------------------------------------
// <copyright file="IAttachmentsStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Database.Data;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
	public interface IAttachmentsStorageActions
	{
		Etag AddAttachment(string key, Etag etag, Stream data, RavenJObject headers);
		void DeleteAttachment(string key, Etag etag);
		Attachment GetAttachment(string key);
        long GetAttachmentsCount();
		IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start);
		IEnumerable<AttachmentInformation> GetAttachmentsAfter(Etag value, int take, long maxTotalSize);
		IEnumerable<AttachmentInformation> GetAttachmentsStartingWith(string idPrefix, int start, int pageSize);
	}
}
