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
		private readonly RamStorageHelper helper;

		public RamDocumentsStorageActions(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
			helper = new RamStorageHelper(state);
		}

		public IEnumerable<JsonDocument> GetDocumentsByReverseUpdateOrder(int start, int take)
		{
			return state.Documents
				.OrderByDescending(pair => pair.Value.Document.LastModified)
				.Select(pair => pair.Value.Document)
				.Skip(start)
				.Take(take);
		}

		public IEnumerable<JsonDocument> GetDocumentsAfter(Guid etag, int take, long? maxSize = null)
		{
			long totalSize = 0;

			return state.Documents
				.OrderBy(pair => pair.Value.Document.Etag)
				.SkipWhile(pair => pair.Value.Document.Etag != null && ((Guid)pair.Value.Document.Etag).CompareTo(etag) > 0)
				.Take(take)
				.TakeWhile(p =>
				{
					var fit = totalSize <= maxSize;
					totalSize += p.Value.Document.SerializedSizeOnDisk;
					return fit;
				})
				.Select(pair => pair.Value.Document);
		}

		public IEnumerable<JsonDocument> GetDocumentsWithIdStartingWith(string idPrefix, int start, int take)
		{
			return
				state.Documents
				.Where(pair => pair.Value.Document.Key.StartsWith(idPrefix, StringComparison.InvariantCultureIgnoreCase))
				.Skip(start)
				.Take(take)
				.Select(pair => pair.Value.Document);
		}

		public long GetDocumentsCount()
		{
			return state.DocumentCount.Value;
		}

		public JsonDocument DocumentByKey(string key, TransactionInformation transactionInformation)
		{
			return DocumentByKeyInternal(key, transactionInformation, (metadata, createDocument) => metadata.Etag != null ? new JsonDocument
			{
				DataAsJson = createDocument(metadata.Key, metadata.Etag.Value, metadata.Metadata),
				Etag = metadata.Etag,
				Key = metadata.Key,
				LastModified = metadata.LastModified,
				Metadata = metadata.Metadata,
				NonAuthoritativeInformation = metadata.NonAuthoritativeInformation,
			} : null);
		}

		public JsonDocumentMetadata DocumentMetadataByKey(string key, TransactionInformation transactionInformation)
		{
			return DocumentByKeyInternal(key, transactionInformation, (metadata, func) => metadata);
		}

		public bool DeleteDocument(string key, Guid? etag, out RavenJObject metadata)
		{
			metadata = null;

			var doc = state.Documents.GetOrDefault(key);

			if (doc == null)
				return false;


			if(doc.Document.Etag != etag)
			{
				throw new ConcurrencyException("DELETE attempted on document '" + key +
				                               "' using a non current etag");
			}

			helper.EnsureNotLockedByTransaction(key, null);

			metadata = doc.Document.Metadata;

			state.Documents.Remove(key);
			state.DocumentCount.Value--;

			return true;
		}

		public Guid AddDocument(string key, Guid? etag, RavenJObject data, RavenJObject metadata)
		{

			if (key != null && Encoding.Unicode.GetByteCount(key) >= 2048)
				throw new ArgumentException(string.Format("The key must be a maximum of 2,048 bytes in Unicode, 1,024 characters, key is: '{0}'", key), "key");

			var doc = state.Documents.GetOrDefault(key);

			var isUpdate = doc != null;

			if (isUpdate)
			{
				helper.EnsureNotLockedByTransaction(key, null);
				helper.EnsureDocumentEtagMatch(key, etag, "PUT");
			}
			else
			{
				if (etag != null && etag != Guid.Empty) // expected something to be there.
					throw new ConcurrencyException("PUT attempted on document '" + key +
												   "' using a non current etag (document deleted)")
					{
						ExpectedETag = etag.Value
					};

				helper.EnsureDocumentIsNotCreatedInAnotherTransaction(key, Guid.NewGuid());

				state.DocumentCount.Value++;
			}
			Guid newEtag = generator.CreateSequentialUuid();

			state.Documents.Set(key, new DocuementWrapper
			{
				Document = new JsonDocument
				{
					Key = key,
					Metadata = metadata,
					Etag = etag,
					DataAsJson = data,
					LastModified = SystemTime.UtcNow
				}
			});

			return newEtag;
		}

		private T DocumentByKeyInternal<T>(string key, TransactionInformation transactionInformation, Func<JsonDocumentMetadata, Func<string, Guid, RavenJObject, RavenJObject>, T> createResult)
			where T : class
		{
			var existsInTx = helper.IsDocumentModifiedInsideTransaction(key);
			var documentsModifiedByTransation = state.DocumentsModifiedByTransations.GetOrDefault(key);

			if (transactionInformation != null && existsInTx)
			{
				var txId = documentsModifiedByTransation.LockByTransaction;
				if (txId == transactionInformation.Id)
				{

					if (documentsModifiedByTransation.DeleteDocument)
						return null;

					var etag = documentsModifiedByTransation.Document.Etag;

					var metadata = documentsModifiedByTransation.Document.Metadata;

					return createResult(new JsonDocumentMetadata
					{
						NonAuthoritativeInformation = false,// we are the transaction, therefor we are Authoritative
						Etag = etag,
						LastModified = documentsModifiedByTransation.Document.LastModified,
						Key = documentsModifiedByTransation.Document.Key,
						Metadata = metadata
					}, (s, guid, arg3) => state.Documents.GetOrDefault(s).Document.DataAsJson);
				}
			}

			var doc = state.Documents.GetOrDefault(key);
		
			if (doc == null)
			{
				if (existsInTx)
				{
					return createResult(new JsonDocumentMetadata
					{
						Etag = Guid.Empty,
						Key = key,
						Metadata = new RavenJObject { { Constants.RavenDocumentDoesNotExists, true } },
						NonAuthoritativeInformation = true,
						LastModified = DateTime.MinValue,
					}, (docKey, etag, metadata) => new RavenJObject());
				}
				return null;
			}

			var existingEtag = doc.Document.Etag;

			return createResult(new JsonDocumentMetadata
			{
				Etag = existingEtag,
				NonAuthoritativeInformation = existsInTx,
				LastModified = doc.Document.LastModified,
				Key = doc.Document.Key,
				Metadata = doc.Document.Metadata
			}, (s, guid, arg3) => state.Documents.GetOrDefault(s).Document.DataAsJson);
		}
	}
}