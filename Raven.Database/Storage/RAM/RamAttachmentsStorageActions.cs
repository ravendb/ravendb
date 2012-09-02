using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Json.Linq;
using System.Linq;

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
			var attachment = state.Attachments.GetOrDefault(key);
			if (attachment != null)
			{
				var existingEtag = attachment.Etag;
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
				state.AttachmentCount.Value += 1;
			}

			var memoryStream = new MemoryStream();
			data.CopyTo(memoryStream);
			var buffer = memoryStream.ToArray();

			Guid newETag = generator.CreateSequentialUuid();
			state.Attachments.Set(key, new Attachment
			{
				Key = key,
				Etag = newETag,
				Metadata = headers,
				Size = buffer.Length,
				Data = () => new MemoryStream(buffer)
			});

			return newETag;
		}

		public void DeleteAttachment(string key, Guid? etag)
		{
			var attachment = state.Attachments.GetOrDefault(key);
			if (attachment == null)
				return;

			var fileEtag = attachment.Etag;

			if (fileEtag != etag && etag != null)
			{
				throw new ConcurrencyException("DELETE attempted on attachment '" + key +
					"' using a non current etag")
				{
					ActualETag = fileEtag,
					ExpectedETag = etag.Value
				};
			}

			state.Attachments.Remove(key);
			state.AttachmentCount.Value--;
		}

		public Attachment GetAttachment(string key)
		{
			return state.Attachments.GetOrDefault(key);
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsByReverseUpdateOrder(int start)
		{
			return state.Attachments
				.OrderByDescending(pair => pair.Value.Etag)
				.Skip(start)
				.Select(pair => new AttachmentInformation
				{
					Key = pair.Key,
					Etag = pair.Value.Etag,
					Metadata = (RavenJObject)pair.Value.Metadata.CloneToken(),
					Size = pair.Value.Size
				});
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsAfter(Guid value, int take, long maxTotalSize)
		{
			long totalSize = 0;

			return state.Attachments
				.OrderBy(pair => pair.Value.Etag)
				.SkipWhile(pair => pair.Value.Etag.CompareTo(value) > 0)
				.Take(take)
				.TakeWhile(p =>
				{
					var fit = totalSize <= maxTotalSize;
					totalSize += p.Value.Size;
					return fit;
				})
				.Select(pair => new AttachmentInformation
				{
					Key = pair.Key,
					Etag = pair.Value.Etag,
					Metadata = (RavenJObject)pair.Value.Metadata.CloneToken(),
					Size = pair.Value.Size
				});
		}

		public IEnumerable<AttachmentInformation> GetAttachmentsStartingWith(string idPrefix, int start, int pageSize)
		{
			return state.Attachments
				.Where(pair => pair.Key.StartsWith(idPrefix, StringComparison.InvariantCultureIgnoreCase))
				.Skip(start)
				.Take(pageSize)
				.Select(pair => new AttachmentInformation
				{
					Key = pair.Key,
					Etag = pair.Value.Etag,
					Metadata = (RavenJObject)pair.Value.Metadata.CloneToken(),
					Size = pair.Value.Size
				});
		}
	}
}