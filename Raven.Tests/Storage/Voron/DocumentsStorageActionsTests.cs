namespace Raven.Tests.Storage.Voron
{
	using System;
	using System.Collections.Generic;
	using System.Linq;

	using Microsoft.Isam.Esent.Interop;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Database.Storage;
	using Raven.Json.Linq;

	using Xunit;
	using Xunit.Extensions;

	[Trait("VoronTest", "StorageActionsTests")]
	[Trait("VoronTest", "DocumentStorage")]
	public class DocumentsStorageActionsTests : TransactionalStorageTestBase
	{
		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_Initialized_CorrectStorageInitialized_DocumentStorageActions_Is_NotNull(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				Assert.Equal(voronStorage.FriendlyName, requestedStorage, StringComparer.InvariantCultureIgnoreCase);
				voronStorage.Batch(accessor => Assert.NotNull(accessor.Documents));
				//TODO : add here "not null" assertions for StorageAction accessors after they were implemented
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetBestNextDocumentEtag_NoDocuments_Returns_OriginalEtag(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				Etag resultEtag = null;
				voronStorage.Batch(viewer => resultEtag = viewer.Documents.GetBestNextDocumentEtag(Etag.Empty));
				Assert.Equal(resultEtag, Etag.Empty);
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetBestNextDocumentEtag_ExistingDocumentEtag_AlreadyTheLargestEtag(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				Etag etag3 = null;
				voronStorage.Batch(mutator =>
					mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()));
				voronStorage.Batch(mutator =>
					mutator.Documents.AddDocument("Foo2", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()));
				voronStorage.Batch(mutator =>
					etag3 = mutator.Documents.AddDocument("Foo3", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()).Etag);

				Etag resultEtag = null;
				voronStorage.Batch(viewer => resultEtag = viewer.Documents.GetBestNextDocumentEtag(etag3));

				Assert.Equal(resultEtag, etag3);
			}
		}


		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetBestNextDocumentEtag_ExistingDocumentEtag(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				Etag etag2 = null;
				Etag etag3 = null;
				voronStorage.Batch(mutator =>
					mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()));
				voronStorage.Batch(mutator =>
					etag2 = mutator.Documents.AddDocument("Foo2", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()).Etag);
				voronStorage.Batch(mutator =>
					etag3 = mutator.Documents.AddDocument("Foo3", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()).Etag);

				Etag resultEtag = null;
				voronStorage.Batch(viewer => resultEtag = viewer.Documents.GetBestNextDocumentEtag(etag2));

				Assert.Equal(resultEtag, etag3);
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetBestNextDocumentEtag_NonExistingDocumentEtag_SmallerThan_MaxDocumentEtag(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				Etag etag2 = null;
				Etag etag3 = null;
				voronStorage.Batch(mutator =>
					mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()));


				voronStorage.Batch(mutator =>
					etag2 = mutator.Documents.AddDocument("Foo2", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()).Etag);

				voronStorage.Batch(mutator =>
					etag3 = mutator.Documents.AddDocument("Foo3", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()).Etag);

				Etag resultEtag = null;
				var nonExistingDocumentEtag = new Etag(etag2.ToString()); //make sure the nonExistingDocumentEtag is between etag2 and etag3
				voronStorage.Batch(viewer => resultEtag = viewer.Documents.GetBestNextDocumentEtag(nonExistingDocumentEtag));

				Assert.Equal(resultEtag, etag3);
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetBestNextDocumentEtag_NonExistingDocumentEtag_LargerThan_MaxDocumentEtag(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				Etag etag2 = null;
				Etag etag3 = null;
				voronStorage.Batch(mutator =>
					mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()));


				voronStorage.Batch(mutator =>
					etag2 = mutator.Documents.AddDocument("Foo2", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()).Etag);

				voronStorage.Batch(mutator =>
					etag3 = mutator.Documents.AddDocument("Foo3", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
						new RavenJObject()).Etag);

				Etag resultEtag = null;
				var nonExistingDocumentEtag = new Etag(UuidType.Documents, 0, 0);
				voronStorage.Batch(viewer => resultEtag = viewer.Documents.GetBestNextDocumentEtag(nonExistingDocumentEtag));

				Assert.Equal(resultEtag, nonExistingDocumentEtag);
			}
		}


		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentAdd_With_InvalidEtag_ExceptionThrown(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator =>
					mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject()));
				Assert.Throws<ConcurrencyException>(() =>
					voronStorage.Batch(mutator =>
						mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject())));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentRead_NonExistingKey_NullReturned(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				JsonDocument document = null;
				voronStorage.Batch(viewer => document = viewer.Documents.DocumentByKey("Foo", null));
				Assert.Null(document);
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentTouch_NonExistingKey_NullEtagsReturned(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				Etag before = null;
				Etag after = null;
				voronStorage.Batch(mutator => mutator.Documents.TouchDocument("Foo", out before, out after));

				Assert.Null(before);
				Assert.Null(after);
			}
		}

		//separate test for a key with forward slashes --> special case in Voron storage implementation --> need to test how internal key parsers manage to do that
		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentAdd_And_DocumentRead(string requestedStorage)
		{
			DocumentStorage_DocumentAdd_And_DocumentRead_Internal(requestedStorage, "Foo/Bar/Test");
			DocumentStorage_DocumentAdd_And_DocumentRead_Internal(requestedStorage, "Foo");
			DocumentStorage_DocumentAdd_And_DocumentRead_Internal(requestedStorage, "Foo/Bar");
		}

		private void DocumentStorage_DocumentAdd_And_DocumentRead_Internal(string requestedStorage, string documentKey)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				AddDocumentResult addResult = null;
				voronStorage.Batch(
					mutator =>
					addResult =
					mutator.Documents.AddDocument(
						documentKey, Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject()));

				RavenJObject document = null;
				JsonDocument jsonDocument = null;
				voronStorage.Batch(
					viewer =>
					{
						jsonDocument = viewer.Documents.DocumentByKey(documentKey, null);
						document = jsonDocument.DataAsJson;
					});

				Assert.NotNull(document);
				Assert.NotNull(jsonDocument);
				Assert.Equal("Bar", document.Value<string>("Name"));

				//verify correctness of LastModified return value
				Assert.True(jsonDocument.LastModified.HasValue);
				// ReSharper disable once PossibleInvalidOperationException (checked HasValue already)
				Assert.Equal(addResult.SavedAt, jsonDocument.LastModified.Value);
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentAdd_And_DocumentDeleted(string requestedStorage)
		{
			DocumentStorage_DocumentAdd_And_DocumentDeleted_Internal(requestedStorage, "Foo");
			DocumentStorage_DocumentAdd_And_DocumentDeleted_Internal(requestedStorage, "Foo/Bar/Test");
		}

		private void DocumentStorage_DocumentAdd_And_DocumentDeleted_Internal(string requestedStorage, string documentKey)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				AddDocumentResult addResult = null;
				voronStorage.Batch(
					mutator =>
					addResult =
					mutator.Documents.AddDocument(
						documentKey,
						Etag.Empty,
						RavenJObject.FromObject(new { Name = "Bar" }),
						RavenJObject.FromObject(new { Meta = "Data" })));

				JsonDocumentMetadata fetchedMetadata = null;
				voronStorage.Batch(viewer => fetchedMetadata = viewer.Documents.DocumentMetadataByKey(documentKey, null));

				Etag deletedEtag = null;
				RavenJObject deletedMetadata = null;
				voronStorage.Batch(
					mutator => mutator.Documents.DeleteDocument(documentKey, null, out deletedMetadata, out deletedEtag));

				JsonDocument documentAfterDelete = null;
				voronStorage.Batch(viewer => documentAfterDelete = viewer.Documents.DocumentByKey(documentKey, null));

				JsonDocumentMetadata metadataAfterDelete = null;
				voronStorage.Batch(viewer => metadataAfterDelete = viewer.Documents.DocumentMetadataByKey(documentKey, null));

				//after delete --> DocumentByKey()/DocumentMetadataByKey() methods should return null
				Assert.Null(documentAfterDelete);
				Assert.Null(metadataAfterDelete);

				Assert.NotNull(deletedEtag);
				Assert.NotNull(deletedMetadata);

				Assert.Equal(addResult.Etag, deletedEtag);
				Assert.Equal(fetchedMetadata.Metadata, deletedMetadata);
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_InsertDocument_And_DocumentRead(string requestedStorage)
		{
			DocumentStorage_InsertDocument_And_DocumentRead_Internal(requestedStorage, "Foo");
			DocumentStorage_InsertDocument_And_DocumentRead_Internal(requestedStorage, "Foo/Bar/Test");
		}

		private void DocumentStorage_InsertDocument_And_DocumentRead_Internal(string requestedStorage, string documentKey)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(
					mutator =>
					mutator.Documents.InsertDocument(
						documentKey, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));

				RavenJObject document = null;
				voronStorage.Batch(viewer => document = viewer.Documents.DocumentByKey(documentKey, null).DataAsJson);

				Assert.NotNull(document);
				Assert.Equal("Bar", document.Value<string>("Name"));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_InsertDocument_Twice_WithCheckForUpdatesTrue_And_DocumentRead(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator => mutator.Documents.InsertDocument("Foo", RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));
				voronStorage.Batch(mutator => mutator.Documents.InsertDocument("Foo", RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));

				RavenJObject document = null;
				voronStorage.Batch(viewer => document = viewer.Documents.DocumentByKey("Foo", null).DataAsJson);

				Assert.NotNull(document);
				Assert.Equal("Bar", document.Value<string>("Name"));
			}
		}


		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentCount_NoDocuments_ZeroReturned(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(viewer =>
				{
					var documentsCount = viewer.Documents.GetDocumentsCount();
					Assert.Equal(0, documentsCount);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentCount_CountReturned(string requestedStorage)
		{
			const int DOCUMENT_COUNT = 10;
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator =>
				{
					for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
						mutator.Documents.AddDocument("Foo" + docIndex, null, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject());
				});
				
				long documentCount = 0;
				voronStorage.Batch(viewer => documentCount = viewer.Documents.GetDocumentsCount());

				Assert.Equal(DOCUMENT_COUNT, documentCount);
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentCount_SomeDocumentsDeleted_CountReturned(string requestedStorage)
		{
			const int DOCUMENT_COUNT = 12;
			const int DOCUMENT_DELETION_COUNT = 4;
			const int DELETION_START_INDEX = DOCUMENT_COUNT / 3;

			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(mutator =>
				{
					for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
						mutator.Documents.AddDocument("Foo" + docIndex,null, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject()); 									
				});

				storage.Batch(mutator =>
				{
					for (int docIndex = DELETION_START_INDEX; docIndex < DELETION_START_INDEX + DOCUMENT_DELETION_COUNT; docIndex++)
					{
						Etag deletedEtag;
						RavenJObject metadata;
						mutator.Documents.DeleteDocument("Foo" + docIndex, null, out metadata, out deletedEtag);
					}
				});

				long documentCount = 0;
				storage.Batch(viewer => documentCount = viewer.Documents.GetDocumentsCount());

				Assert.Equal(DOCUMENT_COUNT - DOCUMENT_DELETION_COUNT, documentCount); //implicitly also check whether DeleteDocument also deletes properly all related document data (from indices)
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_InsertDocument_Twice_WithCheckForUpdatesFalse_ExceptionThrown(string requestedStorage)
		{
			Type expectedException = null;
			if (requestedStorage == "voron")
			{
				expectedException = typeof(ApplicationException);
			}
			else if (requestedStorage == "esent")
			{
				expectedException = typeof(EsentKeyDuplicateException);
			}

			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator => mutator.Documents.InsertDocument("Foo", RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));
				Assert.Throws(expectedException, () =>
					voronStorage.Batch(mutator => mutator.Documents.InsertDocument("Foo", RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), false)));
			}
		}


		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentAdd_And_DocumentRead_Twice_ReturnsCachedObject(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator => mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject()));

				RavenJObject document1 = null, document2 = null;
				voronStorage.Batch(viewer => document1 = viewer.Documents.DocumentByKey("Foo", null).DataAsJson);
				voronStorage.Batch(viewer => document2 = viewer.Documents.DocumentByKey("Foo", null).DataAsJson);

				Assert.NotNull(document1);
				Assert.Equal("Bar", document1.Value<string>("Name"));
				Assert.Equal("Bar", document2.Value<string>("Name"));
			}

		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentAdd_MetadataRead(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(
					mutator =>
						mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }),
							RavenJObject.FromObject(new { Meta = "Data" })));

				JsonDocumentMetadata metadata = null;
				voronStorage.Batch(viewer => metadata = viewer.Documents.DocumentMetadataByKey("Foo", null));

				Assert.NotNull(metadata);
				Assert.NotNull(metadata.Metadata);
				Assert.Equal("Data", metadata.Metadata.Value<string>("Meta"));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_MetadataRead_NonExistingKey_NullReturned(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				JsonDocumentMetadata metadata = null;
				voronStorage.Batch(viewer => metadata = viewer.Documents.DocumentMetadataByKey("Foo", null));
				Assert.Null(metadata);
			}
		}


		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_PutDocumentMetadata_NonExistingKey_ExceptionThrown(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				Assert.Throws<InvalidOperationException>(() => voronStorage.Batch(mutator => mutator.Documents.PutDocumentMetadata("Foo", new RavenJObject())));
			}
		}

		//this test makes sure that document.contains internal check works properly
		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_PutDocumentMetadata_WrongKey_ExceptionThrown(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator => mutator.Documents.AddDocument("Foo", null, RavenJObject.FromObject(new { Name = "Bar" }), RavenJObject.FromObject(new { Meta = "Data" })));

				Assert.Throws<InvalidOperationException>(() => voronStorage.Batch(mutator => mutator.Documents.PutDocumentMetadata("Foo2", new RavenJObject())));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentAdd_DocumentByDifferentKey_NullReturned(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator => mutator.Documents.AddDocument("Foo", null, RavenJObject.FromObject(new { Name = "Bar" }), RavenJObject.FromObject(new { Meta = "Data" })));

				JsonDocumentMetadata metadata = null;
				Assert.DoesNotThrow(
					() => voronStorage.Batch(viewer => metadata = viewer.Documents.DocumentMetadataByKey("Foo2", null)));

				Assert.Equal(null, metadata);

			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentAdd_MetadataUpdated_MetadataRead(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator => mutator.Documents.AddDocument("Foo", null, RavenJObject.FromObject(new { Name = "Bar" }), RavenJObject.FromObject(new { Meta = "Data" })));

				voronStorage.Batch(mutator => mutator.Documents.AddDocument("Foo", null, RavenJObject.FromObject(new { Name = "Bar" }), RavenJObject.FromObject(new { Meta = "NotData" })));


				JsonDocumentMetadata metadata = null;
				voronStorage.Batch(viewer => metadata = viewer.Documents.DocumentMetadataByKey("Foo", null));
				Assert.Equal("NotData", metadata.Metadata.Value<string>("Meta"));

			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentUpdate_WithSpecifiedEtag_And_DocumentRead(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				const string TEST_DOCUMENT_KEY = "Foo";
				const string TEST_DOCUMENT_EXPECTED_NAME = "Bar2";

				//add document
				AddDocumentResult addResult = null;
				voronStorage.Batch(mutator => addResult = mutator.Documents.AddDocument(TEST_DOCUMENT_KEY, Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject()));

				//then update document
				voronStorage.Batch(mutator => mutator.Documents.AddDocument(TEST_DOCUMENT_KEY, addResult.Etag, RavenJObject.FromObject(new { Name = TEST_DOCUMENT_EXPECTED_NAME }), new RavenJObject()));

				RavenJObject document = null;
				voronStorage.Batch(viewer => document = viewer.Documents.DocumentByKey(TEST_DOCUMENT_KEY, null).DataAsJson);

				Assert.NotNull(document);
				Assert.Equal(TEST_DOCUMENT_EXPECTED_NAME, document.Value<string>("Name"));
			}

		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_DocumentTouch_EtagUpdated(string requestedStorage)
		{
			const string TEST_DOCUMENT_KEY = "Foo";
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				AddDocumentResult initialAddResult = null;
				voronStorage.Batch(mutator => initialAddResult = mutator.Documents.AddDocument(TEST_DOCUMENT_KEY, Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject()));

				Etag preTouchEtag = null;
				Etag afterTouchEtag = null;
				voronStorage.Batch(mutator => mutator.Documents.TouchDocument(TEST_DOCUMENT_KEY, out preTouchEtag, out afterTouchEtag));

				Assert.NotNull(preTouchEtag);
				Assert.NotNull(afterTouchEtag);

				Assert.Equal(initialAddResult.Etag, preTouchEtag);
				Assert.NotEqual(preTouchEtag, afterTouchEtag);
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsAfter_NoDocuments_EmptyCollectionReturned(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(viewer => Assert.Empty(viewer.Documents.GetDocumentsAfter(Etag.Empty, 25)));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsAfter(string requestedStorage)
		{
			const int DOCUMENT_COUNT = 12;
			const int DOCUMENT_SKIP_COUNT = DOCUMENT_COUNT / 4;

			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				var documentAddResults = new List<AddDocumentResult>();
				voronStorage.Batch(mutator =>
				{
					for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
						documentAddResults.Add(mutator.Documents.InsertDocument("Foo" + docIndex,
							RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));
				});

				documentAddResults = documentAddResults.OrderBy(row => row.Etag).ToList();

				var documentEtagsAfterSkip =
					documentAddResults.Select(row => row.Etag).Skip(DOCUMENT_SKIP_COUNT).ToArray();

				IList<JsonDocument> fetchedDocuments = null;
				voronStorage.Batch(
					viewer =>
						fetchedDocuments =
							viewer.Documents
							.GetDocumentsAfter(documentEtagsAfterSkip.First(), documentEtagsAfterSkip.Length)
							.ToList());

				var fetchedEtagList = fetchedDocuments.OrderBy(row => row.Etag)
													  .Select(row => row.Etag)
													  .ToList();

				Assert.Empty(fetchedEtagList.Except(documentEtagsAfterSkip.Skip(1)));
				Assert.Empty(documentEtagsAfterSkip.Skip(1).Except(fetchedEtagList));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsAfter_With_EtagUntil(string requestedStorage)
		{
			const int DOCUMENT_COUNT = 12;
			const int DOCUMENT_SKIP_COUNT = DOCUMENT_COUNT / 4;

			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				var documentAddResults = new List<AddDocumentResult>();
				voronStorage.Batch(mutator =>
				{
					for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
						documentAddResults.Add(mutator.Documents.InsertDocument("Foo" + docIndex,
							RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));
				});

				documentAddResults = documentAddResults.OrderBy(row => row.Etag).ToList();

				var documentEtagsAfterSkip = documentAddResults.Select(row => row.Etag)
					.Skip(DOCUMENT_SKIP_COUNT)
					.Take(DOCUMENT_COUNT - (DOCUMENT_SKIP_COUNT * 2))
					.ToArray();

				IList<JsonDocument> fetchedDocuments = null;
				voronStorage.Batch(
					viewer =>
						fetchedDocuments =
							viewer.Documents
							.GetDocumentsAfter(documentEtagsAfterSkip.First(), documentEtagsAfterSkip.Length, null, documentEtagsAfterSkip.Last())
							.ToList());

				var fetchedEtagList = fetchedDocuments.OrderBy(row => row.Etag)
													  .Select(row => row.Etag)
													  .ToList();

				Assert.Empty(fetchedEtagList.Except(documentEtagsAfterSkip.Skip(1)));
				Assert.Empty(documentEtagsAfterSkip.Skip(1).Except(fetchedEtagList));

			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsAfter_With_Take(string requestedStorage)
		{
			const int DOCUMENT_COUNT = 12;
			const int DOCUMENT_SKIP_COUNT = DOCUMENT_COUNT / 4;
			const int DOCUMENT_TAKE_VALUE = DOCUMENT_COUNT / 3;

			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				var documentAddResults = new List<AddDocumentResult>();
				voronStorage.Batch(mutator =>
				{
					for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
						documentAddResults.Add(mutator.Documents.InsertDocument("Foo" + docIndex,
							RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));
				});

				documentAddResults = documentAddResults.OrderBy(row => row.Etag).ToList();

				var documentEtagsAfterSkip = documentAddResults.Select(row => row.Etag)
					.Skip(DOCUMENT_SKIP_COUNT)
					.Take(DOCUMENT_COUNT - (DOCUMENT_SKIP_COUNT * 2))
					.ToArray();

				var expectedDocumentSet = documentEtagsAfterSkip.Skip(1)
																.Take(DOCUMENT_TAKE_VALUE)
																.ToList();

				IList<JsonDocument> fetchedDocuments = null;
				voronStorage.Batch(
					viewer =>
						fetchedDocuments =
							viewer.Documents
							.GetDocumentsAfter(documentEtagsAfterSkip.First(), DOCUMENT_TAKE_VALUE, null, documentEtagsAfterSkip.Last())
							.ToList());

				var fetchedEtagList = fetchedDocuments.OrderBy(row => row.Etag)
													  .Select(row => row.Etag)
													  .ToList();

				Assert.Empty(fetchedEtagList.Except(expectedDocumentSet));
				Assert.Empty(expectedDocumentSet.Except(fetchedEtagList));

			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsAfter_With_MaxSize(string requestedStorage)
		{
			const int DOCUMENT_COUNT = 12;
			const int DOCUMENT_SKIP_COUNT = DOCUMENT_COUNT / 4;
			const int MAX_SIZE = 100;

			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator =>
				{
					for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
						mutator.Documents.InsertDocument("Foo" + docIndex, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true);
				});

				var existingDocuments = new List<JsonDocument>();
				voronStorage.Batch(viewer =>
				{
					for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
						existingDocuments.Add(viewer.Documents.DocumentByKey("Foo" + docIndex, null));
				});

				var documentAfterSkip = existingDocuments
					.OrderBy(row => row.Etag)
					.Skip(DOCUMENT_SKIP_COUNT)
					.Take(DOCUMENT_COUNT - (DOCUMENT_SKIP_COUNT * 2))
					.ToArray();

				var expectedDocumentSet = new List<JsonDocument>();
				var totalDocumentSizeSoFar = 0;
				foreach (var document in documentAfterSkip.Skip(1))
				{
					totalDocumentSizeSoFar += document.SerializedSizeOnDisk;
					expectedDocumentSet.Add(document);
					if (totalDocumentSizeSoFar >= MAX_SIZE)
						break;
				}

				IList<JsonDocument> fetchedDocuments = null;
				voronStorage.Batch(
					viewer =>
						fetchedDocuments =
							viewer.Documents
							.GetDocumentsAfter(documentAfterSkip.First().Etag, documentAfterSkip.Length, MAX_SIZE, documentAfterSkip.Last().Etag)
							.ToList());

				var fetchedEtagList = fetchedDocuments.OrderBy(row => row.Etag)
													   .Select(row => row.Etag)
													   .ToList();

				Assert.Empty(fetchedEtagList.Except(expectedDocumentSet.Select(row => row.Etag)));
				Assert.Empty(expectedDocumentSet.Select(row => row.Etag).Except(fetchedEtagList));

			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsByReverseUpdateOrder_RetrievedWithCorrectOrder(string requestedStorage)
		{
			const int DOCUMENT_COUNT = 12;

			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				var documentAddResults = new List<AddDocumentResult>();
				voronStorage.Batch(mutator =>
				{
					for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
						documentAddResults.Add(mutator.Documents.InsertDocument("Foo" + docIndex,
							RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));
				});

				documentAddResults.Reverse();

				List<JsonDocument> documentReverseOrderFetchResults = null;
				voronStorage.Batch(viewer => documentReverseOrderFetchResults = viewer.Documents.GetDocumentsByReverseUpdateOrder(0, documentAddResults.Count).ToList());

				Assert.NotNull(documentReverseOrderFetchResults);

				for (var etagIndex = 0; etagIndex < documentAddResults.Count; etagIndex++)
				{
					Assert.True(documentAddResults[etagIndex].Etag.Equals(documentReverseOrderFetchResults[etagIndex].Etag));
				}
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsByReverseUpdateOrder_NoDocuments_EmptyCollectionReturned(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(viewer =>
				{
					var fetchedDocuments = viewer.Documents.GetDocumentsByReverseUpdateOrder(0, 125);
					Assert.Empty(fetchedDocuments);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsByReverseUpdateOrder_RetrievedWithCorrectOrder_DifferentSkipParameters(string requestedStorage)
		{
			DocumentStorage_GetDocumentsByReverseUpdateOrder_RetrievedWithCorrectOrder_DifferentSkipParameters_Internal(requestedStorage, 15, 0);
			DocumentStorage_GetDocumentsByReverseUpdateOrder_RetrievedWithCorrectOrder_DifferentSkipParameters_Internal(requestedStorage, 15, 5);
			DocumentStorage_GetDocumentsByReverseUpdateOrder_RetrievedWithCorrectOrder_DifferentSkipParameters_Internal(requestedStorage, 5, 7);
		}

		private void DocumentStorage_GetDocumentsByReverseUpdateOrder_RetrievedWithCorrectOrder_DifferentSkipParameters_Internal(string requestedStorage,
			int itemCount, int skip)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				var documentAddResults = new List<AddDocumentResult>();
				voronStorage.Batch(
					mutator =>
					{
						for (int docIndex = 0; docIndex < itemCount; docIndex++)
						{
							// ReSharper disable once AccessToModifiedClosure
							documentAddResults.Add(
								mutator.Documents.InsertDocument(
									"Foo" + docIndex, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));
						}
					});

				documentAddResults.Reverse();
				documentAddResults = documentAddResults.Skip(skip).ToList();

				List<JsonDocument> documentReverseOrderFetchResults = null;
				voronStorage.Batch(
					viewer =>
					documentReverseOrderFetchResults =
					viewer.Documents.GetDocumentsByReverseUpdateOrder(skip, documentAddResults.Count).ToList());

				Assert.NotNull(documentReverseOrderFetchResults);

				if (skip >= itemCount)
				{
					Assert.Empty(documentReverseOrderFetchResults);
				}

				for (var etagIndex = 0; etagIndex < documentAddResults.Count; etagIndex++)
				{
					Assert.True(documentAddResults[etagIndex].Etag.Equals(documentReverseOrderFetchResults[etagIndex].Etag));
				}
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsWithIdStartingWith(string requestedStorage)
		{
			DocumentStorage_GetDocumentsWithIdStartingWith_Internal(requestedStorage, 25, 0, 20);
			DocumentStorage_GetDocumentsWithIdStartingWith_Internal(requestedStorage, 25, 5, 10);
			DocumentStorage_GetDocumentsWithIdStartingWith_Internal(requestedStorage, 15, 3, 6);
			DocumentStorage_GetDocumentsWithIdStartingWith_Internal(requestedStorage, 5, 0, 0);
		}

		private void DocumentStorage_GetDocumentsWithIdStartingWith_Internal(string requestedStorage, int itemCount, int start, int take)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				var inputData = new Dictionary<string, RavenJObject>();
				voronStorage.Batch(
					mutator =>
					{
						for (int itemIndex = 0; itemIndex < itemCount; itemIndex++)
						{
							var keyPrefix = (itemIndex % 2 == 0) ? "Foo" : "Bar";
							var document = RavenJObject.FromObject(new { Name = "Bar" + itemIndex });

							mutator.Documents.AddDocument(keyPrefix + itemIndex, Etag.Empty, document, new RavenJObject());
							inputData.Add(keyPrefix + itemIndex, document);
						}
					});

				IList<JsonDocument> fetchedDocuments = null;
				voronStorage.Batch(
					viewer => fetchedDocuments = viewer.Documents.GetDocumentsWithIdStartingWith("Bar", start, take).ToList());

				Assert.NotNull(fetchedDocuments);
				var relevantInputKeys =
					new HashSet<string>(
						inputData.Where(kvp => kvp.Key.StartsWith("Bar"))
								 .OrderBy(kvp => kvp.Key)
								 .Select(row => row.Key)
								 .Skip(start)
								 .Take(take));

				var fetchedDocumentKeys = new HashSet<string>(fetchedDocuments.Select(doc => doc.Key));

				Assert.True(fetchedDocumentKeys.Count <= take);
				Assert.True(fetchedDocumentKeys.SetEquals(relevantInputKeys));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsWithIdStartingWith_NoDocuments_EmptyCollectionReturned(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(viewer => Assert.Empty(viewer.Documents.GetDocumentsWithIdStartingWith("Foo", 0, 25).ToList()));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsWithIdStartingWith_WithNonZeroStart(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator =>
				{
					mutator.Documents.AddDocument("Foo1", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar11" }), new RavenJObject());
					mutator.Documents.AddDocument("Bar1", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar22" }), new RavenJObject());
					mutator.Documents.AddDocument("Foo2", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar33" }), new RavenJObject());
					mutator.Documents.AddDocument("Bar2", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar44" }), new RavenJObject());
					mutator.Documents.AddDocument("Foo3", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar55" }), new RavenJObject());
					mutator.Documents.AddDocument("Bar3", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar66" }), new RavenJObject());
				});

				IList<JsonDocument> fetchedDocuments = null;
				voronStorage.Batch(viewer => fetchedDocuments = viewer.Documents.GetDocumentsWithIdStartingWith("Bar", 1, 3).ToList());

				Assert.NotNull(fetchedDocuments);

				Assert.True(fetchedDocuments.Count == 2);
				Assert.True(fetchedDocuments.Any(row => row.Key == "Bar2"));
				Assert.True(fetchedDocuments.Any(row => row.Key == "Bar3"));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public void DocumentStorage_GetDocumentsWithIdStartingWith_WithTake(string requestedStorage)
		{
			using (var voronStorage = NewTransactionalStorage(requestedStorage))
			{
				voronStorage.Batch(mutator =>
				{
					mutator.Documents.AddDocument("Foo1", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar11" }), new RavenJObject());
					mutator.Documents.AddDocument("Bar1", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar22" }), new RavenJObject());
					mutator.Documents.AddDocument("Foo2", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar33" }), new RavenJObject());
					mutator.Documents.AddDocument("Bar2", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar44" }), new RavenJObject());
					mutator.Documents.AddDocument("Foo3", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar55" }), new RavenJObject());
					mutator.Documents.AddDocument("Bar3", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar66" }), new RavenJObject());
					mutator.Documents.AddDocument("Foo4", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar66" }), new RavenJObject());
					mutator.Documents.AddDocument("Bar4", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar77" }), new RavenJObject());
					mutator.Documents.AddDocument("Foo5", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar66" }), new RavenJObject());
					mutator.Documents.AddDocument("Bar5", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar77" }), new RavenJObject());
					mutator.Documents.AddDocument("Foo6", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar66" }), new RavenJObject());
					mutator.Documents.AddDocument("Bar6", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar77" }), new RavenJObject());
				});

				IList<JsonDocument> fetchedDocuments = null;
				voronStorage.Batch(viewer => fetchedDocuments = viewer.Documents.GetDocumentsWithIdStartingWith("Bar", 2, 3).ToList());

				Assert.NotNull(fetchedDocuments);

				Assert.True(fetchedDocuments.Count == 3);
				Assert.True(fetchedDocuments.Any(row => row.Key == "Bar3"));
				Assert.True(fetchedDocuments.Any(row => row.Key == "Bar4"));
				Assert.True(fetchedDocuments.Any(row => row.Key == "Bar5"));
			}
		}
	}
}
