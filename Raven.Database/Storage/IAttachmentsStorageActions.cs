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
		Guid AddAttachment(string key, Guid? etag, Stream data, RavenJObject headers);
		void DeleteAttachment(string key, Guid? etag);
		Attachment GetAttachment(string key);
		IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start);
		IEnumerable<AttachmentInformation> GetAttachmentsAfter(Guid value, int take);
		IEnumerable<AttachmentInformation> GetAttachmentsStartingWith(string idPrefix, int start, int pageSize);
	}
}
