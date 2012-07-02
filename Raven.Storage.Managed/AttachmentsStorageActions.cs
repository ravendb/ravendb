//-----------------------------------------------------------------------
// <copyright file="AttachmentsStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
	public class AttachmentsStorageActions : IAttachmentsStorageActions
	{
		private readonly TableStorage storage;
		private readonly IUuidGenerator generator;
		private static readonly Logger logger = LogManager.GetCurrentClassLogger();

		public AttachmentsStorageActions(TableStorage storage, IUuidGenerator generator)
		{
			this.storage = storage;
			this.generator = generator;
		}

		public Guid AddAttachment(string key, Guid? etag, Stream data, RavenJObject headers)
		{
			AssertValidEtag(key, etag, "PUT");

			var ms = new MemoryStream();
			headers.WriteTo(ms);
			data.CopyTo(ms);
			var newEtag = generator.CreateSequentialUuid();
			var result = storage.Attachments.Put(new RavenJObject
			{
				{"key", key},
				{"etag", newEtag.ToByteArray()}
			}, ms.ToArray());
		   if (result == false)
			   throw new ConcurrencyException("PUT attempted on attachment '" + key + "' while it was locked by another transaction");
			logger.Debug("Adding attachment {0}", key);
			return newEtag;
		}

		private void AssertValidEtag(string key, Guid? etag, string op)
		{
			var readResult =
				storage.Attachments.Read(new RavenJObject { { "key", key } });

			if(readResult != null && etag != null)
			{
				var existingEtag = new Guid(readResult.Key.Value<byte[]>("etag"));
				if (existingEtag != etag)
				{
					throw new ConcurrencyException(op + " attempted on attachment '" + key +
												   "' using a non current etag")
					{
						ActualETag = existingEtag,
						ExpectedETag = etag.Value
					};
				}
			}
		}

		public void DeleteAttachment(string key, Guid? etag)
		{
			AssertValidEtag(key, etag, "DELETE");

			if (!storage.Attachments.Remove(new RavenJObject { { "key", key } }))
				throw new ConcurrencyException("DELETE attempted on attachment '" + key +
											   "'  while it was locked by another transaction");
			logger.Debug("Attachment with key '{0}' was deleted", key);
		}

		public Attachment GetAttachment(string key)
		{
			var readResult = storage.Attachments.Read(new RavenJObject { { "key", key } });
			if (readResult == null)
				return null;
			var attachmentDAta = readResult.Data();
			var memoryStream = new MemoryStream(attachmentDAta);
			var metadata = memoryStream.ToJObject();
			return new Attachment
			{
				Key = key,
				Etag = new Guid(readResult.Key.Value<byte[]>("etag")),
				Metadata = metadata,
				Data = () => memoryStream,
				Size = (int)(memoryStream.Length - memoryStream.Position)
			};
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start)
		{
			return from key in storage.Attachments["ByEtag"].SkipFromEnd(start)
				   let attachment = GetAttachment(key.Value<string>("key"))
				   select new AttachmentInformation
				   {
					   Key = key.Value<string>("key"),
					   Etag = new Guid(key.Value<byte[]>("etag")),
					   Metadata = attachment.Metadata,
					   Size = attachment.Size
				   };
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsStartingWith(string idPrefix, int start, int pageSize)
		{
			return from key in storage.Attachments["ByKey"].SkipTo(new RavenJObject {{"key", idPrefix}})
			       	.Skip(start)
			       	.TakeWhile(x => x.Value<string>("key").StartsWith(idPrefix))
			       	.Take(pageSize)
			       let attachment = GetAttachment(key.Value<string>("key"))
			       select new AttachmentInformation
			       {
			       	Key = key.Value<string>("key"),
			       	Etag = new Guid(key.Value<byte[]>("etag")),
			       	Metadata = attachment.Metadata,
			       	Size = attachment.Size
			       };
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsAfter(Guid value, int take)
		{
			return from key in storage.Attachments["ByEtag"]
			       	.SkipAfter(new RavenJObject {{"etag", value.ToByteArray()}})
			       	.Take(take)
			       let attachment = GetAttachment(key.Value<string>("key"))
			       select new AttachmentInformation
			       {
			       	Key = key.Value<string>("key"),
			       	Etag = new Guid(key.Value<byte[]>("etag")),
			       	Metadata = attachment.Metadata,
			       	Size = attachment.Size
			       };
		}
	}
}