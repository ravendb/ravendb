using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Database.Impl;
using Raven.Json.Linq;

namespace Raven.Database.Storage.RAM
{
	class RamDocumentsStorageActions : IDocumentStorageActions
	{
		private readonly RamState state;
		private readonly IUuidGenerator generator;

		public RamDocumentsStorageActions(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
		}

		public IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take)
		{
			return state.Documents
				.OrderByDescending(document => document.LastModified)
				.Skip(start)
				.Take(take);
		}

		public IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag, int take, long? maxSize = null)
		{
			long totalSize = 0;

			return state.Documents
				.OrderBy(document => document.Etag)
				.SkipWhile(document => document.Etag != null && ((Guid) document.Etag).CompareTo(etag) > 0)
				.Take(take)
				.TakeWhile(p =>
				{
					var fit = totalSize <= maxSize;
					totalSize += p.SerializedSizeOnDisk;
					return fit;
				});
		}

		public IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take)
		{
			return
				state.Documents
				.Where(document => document.Key.StartsWith(idPrefix, StringComparison.InvariantCultureIgnoreCase))
				.Skip(start)
				.Take(take);
		}

		public long GetDocumentsCount()
		{
			return state.DocumentCount.Value;
		}

		public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
		{
			//TODO: check transaction

			return state.Documents.FirstOrDefault(document => document.Key == key);
		}

		public JsonDocumentMetadata DocumentMetadataByKey(string key, TransactionInformation transactionInformation)
		{
			var doc =  state.Documents.FirstOrDefault(document => document.Key == key);

			if (doc == null)
				return null;

			//TODO: check transaction

			return new JsonDocumentMetadata
			{
				Etag = doc.Etag,
				Key = doc.Key,
				LastModified = doc.LastModified,
				Metadata = doc.Metadata,
				NonAuthoritativeInformation = doc.NonAuthoritativeInformation
			};
		}

		public bool DeleteDocument(string key, Guid? etag, out RavenJObject metadata)
		{
			metadata = null;
			var doc = state.Documents.FirstOrDefault(document => document.Key == key);

			if (doc == null)
				return false;

			var existingEtag = doc.Etag;
			if (existingEtag != etag && etag != null && existingEtag != null)
			{
				return false;
			}

			//TODO: check transaction

			metadata = doc.Metadata;

			state.Documents.Remove(doc);
			state.DocumentCount.Value--;

			return true;
		}

		public Guid AddDocument(string key, Guid? etag, RavenJObject data, RavenJObject metadata)
		{
			var docuemnt = state.Documents.FirstOrDefault(document => document.Key == key);
			if (docuemnt != null)
			{
				var existingEtag = docuemnt.Etag;
				if (existingEtag != etag && etag != null && existingEtag != null)
				{
					throw new ConcurrencyException("PUT attempted on document '" + key +
						"' using a non current etag")
					{
						ActualETag = (Guid)existingEtag,
						ExpectedETag = etag.Value
					};
				}
			}
			else
			{
				//TODO: check transaction

				state.DocumentCount.Value++;
			}

			Guid newETag = generator.CreateSequentialUuid();
			state.Documents.Add(new JsonDocument
			{
				Key = key,
				Etag = newETag,
				DataAsJson = data,
				LastModified = SystemTime.UtcNow,
				Metadata = metadata,
			});

			return newETag;
		}
	}
}