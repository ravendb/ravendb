using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Permissions;
using System.Security.Principal;
using System.Web.UI.WebControls;
using System.Web.WebSockets;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Tests.Bugs.Indexing;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Storage.Voron
{
	[Trait("VoronTest","DocumentStorage")]
    public class DocumentsStorageActionsTests : RavenTest
    {
        private readonly string baseDataFolder = AppDomain.CurrentDomain.BaseDirectory;

        private ITransactionalStorage NewVoronStorage()
        {
            return NewTransactionalStorage("voron", baseDataFolder);
        }

        [Fact]
        public void DocumentStorage_Initialized_CorrectStorageInitialized_DocumentStorageActions_Is_NotNull()
        {
            using (var voronStorage = NewVoronStorage())
            {                
                Assert.Equal(voronStorage.FriendlyName,"Voron");
                voronStorage.Batch(accessor => Assert.NotNull(accessor.Documents));
                //TODO : add here "not null" assertions for StorageAction accessors after they were implemented
            }
        }

	    [Fact]
	    public void DocumentStorage_GetBestNextDocumentEtag_NoDocuments_Returns_OriginalEtag()
	    {
	        using (var voronStorage = NewVoronStorage())
	        {
                Etag resultEtag = null;
                voronStorage.Batch(viewer => resultEtag = viewer.Documents.GetBestNextDocumentEtag(Etag.Empty));
                Assert.Equal(resultEtag,Etag.Empty);
            }	        
	    }

        [Fact]
        public void DocumentStorage_GetBestNextDocumentEtag_ExistingDocumentEtag_AlreadyTheLargestEtag()
        {
            using (var voronStorage = NewVoronStorage())
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


	    [Fact]
        public void DocumentStorage_GetBestNextDocumentEtag_ExistingDocumentEtag()
	    {
	        using (var voronStorage = NewVoronStorage())
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

                Assert.Equal(resultEtag,etag3);
	        }
	    }

        [Fact]
        public void DocumentStorage_GetBestNextDocumentEtag_NonExistingDocumentEtag_SmallerThan_MaxDocumentEtag()
        {
            using (var voronStorage = NewVoronStorage())
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

        [Fact]
        public void DocumentStorage_GetBestNextDocumentEtag_NonExistingDocumentEtag_LargerThan_MaxDocumentEtag()
        {
            using (var voronStorage = NewVoronStorage())
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


        [Fact]
        public void DocumentStorage_DocumentAdd_With_InvalidEtag_ExceptionThrown()
        {
            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator => 
                    mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new {Name = "Bar"}), new RavenJObject()));
                Assert.Throws<ConcurrencyException>(() => 
                    voronStorage.Batch(mutator => 
                        mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new {Name = "Bar"}), new RavenJObject())));
            }
        }

        [Fact]
        public void DocumentStorage_DocumentRead_NonExistingKey_NullReturned()
        {
            using (var voronStorage = NewVoronStorage())
            {
                JsonDocument document = null;
                voronStorage.Batch(viewer => document = viewer.Documents.DocumentByKey("Foo", null));
                Assert.Null(document);
            }
        }

        [Fact]
        public void DocumentStorage_DocumentTouch_NonExistingKey_NullEtagsReturned()
        {
            using (var voronStorage = NewVoronStorage())
            {
                Etag before = null;
                Etag after = null;
                voronStorage.Batch(mutator => mutator.Documents.TouchDocument("Foo",out before, out after));

                Assert.Null(before);
                Assert.Null(after);
            }
        }

        //separate test for a key with forward slashes --> special case in Voron storage implementation --> need to test how internal key parsers manage to do that
        [Theory]
        [InlineData("Foo/Bar/Test")]
        [InlineData("Foo")]
        [InlineData("Foo/Bar")]
        public void DocumentStorage_DocumentAdd_And_DocumentRead(string documentKey)
        {
            using (var voronStorage = NewVoronStorage())
            {
                AddDocumentResult addResult = null;
                voronStorage.Batch(mutator => addResult = mutator.Documents.AddDocument(documentKey, Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject()));

                RavenJObject document = null;
                JsonDocument jsonDocument = null;
                voronStorage.Batch(viewer =>
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
        [InlineData("Foo")]
        [InlineData("Foo/Bar/Test")]
        public void DocumentStorage_DocumentAdd_And_DocumentDeleted(string documentKey)
        {
            using (var voronStorage = NewVoronStorage())
            {
                AddDocumentResult addResult = null;
                voronStorage.Batch(mutator =>
                    addResult = mutator.Documents.AddDocument(documentKey, Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }), RavenJObject.FromObject(new { Meta = "Data" })));

                JsonDocumentMetadata fetchedMetadata = null;
                voronStorage.Batch(viewer => fetchedMetadata = viewer.Documents.DocumentMetadataByKey(documentKey, null));

                Etag deletedEtag = null;
                RavenJObject deletedMetadata = null;
                voronStorage.Batch(mutator => mutator.Documents.DeleteDocument(documentKey, null, out deletedMetadata, out deletedEtag));

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
        [InlineData("Foo")]
        [InlineData("Foo/Bar/Test")]
        public void DocumentStorage_InsertDocument_And_DocumentRead(string documentKey)
        {
            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator => mutator.Documents.InsertDocument(documentKey, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));

                RavenJObject document = null;
                voronStorage.Batch(viewer => document = viewer.Documents.DocumentByKey(documentKey, null).DataAsJson);

                Assert.NotNull(document);
                Assert.Equal("Bar", document.Value<string>("Name"));
            }
        }

        [Fact]
        public void DocumentStorage_InsertDocument_Twice_WithCheckForUpdatesTrue_And_DocumentRead()
        {
            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator => mutator.Documents.InsertDocument("Foo", RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));
                voronStorage.Batch(mutator => mutator.Documents.InsertDocument("Foo", RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));

                RavenJObject document = null;
                voronStorage.Batch(viewer => document = viewer.Documents.DocumentByKey("Foo", null).DataAsJson);

                Assert.NotNull(document);
                Assert.Equal("Bar", document.Value<string>("Name"));
            }
        }


	    [Fact]
	    public void DocumentStorage_GetDocumentCount_NoDocuments_ZeroReturned()
	    {
	        using (var voronStorage = NewVoronStorage())
	        {
                voronStorage.Batch(viewer =>
                {                    
                    var documentsCount = viewer.Documents.GetDocumentsCount();
                    Assert.Equal(0,documentsCount);
                });
	        }
	    }

	    [Fact]
        public void DocumentStorage_GetDocumentCount_CountReturned()
        {
            const int DOCUMENT_COUNT = 10;
            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator =>
                {
                    for(int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
                        mutator.Documents.InsertDocument("Foo" + docIndex, RavenJObject.FromObject(new {Name = "Bar"}),new RavenJObject(), true);
                });

                long documentCount = 0;
                voronStorage.Batch(viewer => documentCount = viewer.Documents.GetDocumentsCount());

                Assert.Equal(DOCUMENT_COUNT, documentCount);
            }
        }

        [Fact]
        public void DocumentStorage_GetDocumentCount_SomeDocumentsDeleted_CountReturned()
        {
            const int DOCUMENT_COUNT = 12;
            const int DOCUMENT_DELETION_COUNT = 4;
            const int DELETION_START_INDEX = DOCUMENT_COUNT / 3;

            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator =>
                {
                    for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
                        mutator.Documents.InsertDocument("Foo" + docIndex, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true);
                });

                voronStorage.Batch(mutator =>
                {
                    for (int docIndex = DELETION_START_INDEX; docIndex < DELETION_START_INDEX + DOCUMENT_DELETION_COUNT; docIndex++)
                    {
                        Etag deletedEtag;
                        RavenJObject metadata;
                        mutator.Documents.DeleteDocument("Foo" + docIndex, null,out metadata,out deletedEtag);
                    }
                });

                long documentCount = 0;
                voronStorage.Batch(viewer => documentCount = viewer.Documents.GetDocumentsCount());

                Assert.Equal(DOCUMENT_COUNT - DOCUMENT_DELETION_COUNT, documentCount); //implicitly also check whether DeleteDocument also deletes properly all related document data (from indices)
            }
        }

        [Fact]
        public void DocumentStorage_InsertDocument_Twice_WithCheckForUpdatesFalse_ExceptionThrown()
        {
            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator => mutator.Documents.InsertDocument("Foo", RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));
                Assert.Throws<ApplicationException>(() => 
                    voronStorage.Batch(mutator => mutator.Documents.InsertDocument("Foo", RavenJObject.FromObject(new {Name = "Bar"}), new RavenJObject(), false)));
            }
        }


        [Fact]
        public void DocumentStorage_DocumentAdd_And_DocumentRead_Twice_ReturnsCachedObject()
        {
            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator => mutator.Documents.AddDocument("Foo", Etag.Empty, RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject()));

                RavenJObject document1 = null,document2 = null;
                voronStorage.Batch(viewer => document1 = viewer.Documents.DocumentByKey("Foo", null).DataAsJson);
                voronStorage.Batch(viewer => document2 = viewer.Documents.DocumentByKey("Foo", null).DataAsJson);

                Assert.NotNull(document1);
                Assert.Equal("Bar", document1.Value<string>("Name"));
                Assert.Equal("Bar", document2.Value<string>("Name"));
            }

        }

        [Fact]
        public void DocumentStorage_DocumentAdd_MetadataRead()
        {
            using (var voronStorage = NewVoronStorage())
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

        [Fact]
        public void DocumentStorage_MetadataRead_NonExistingKey_NullReturned()
        {
            using (var voronStorage = NewVoronStorage())
            {
                JsonDocumentMetadata metadata = null;
                voronStorage.Batch(viewer => metadata = viewer.Documents.DocumentMetadataByKey("Foo", null));
                Assert.Null(metadata);
            }
        }

        
        [Fact]
        public void DocumentStorage_PutDocumentMetadata_NonExistingKey_ExceptionThrown()
        {
            using (var voronStorage = NewVoronStorage())
            {
                Assert.Throws<InvalidOperationException>(() => voronStorage.Batch(mutator => mutator.Documents.PutDocumentMetadata("Foo", new RavenJObject())));
            }
        }

        //this test makes sure that document.contains internal check works properly
        [Fact]
        public void DocumentStorage_PutDocumentMetadata_WrongKey_ExceptionThrown()
        {
            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator => mutator.Documents.AddDocument("Foo", null, RavenJObject.FromObject(new { Name = "Bar" }), RavenJObject.FromObject(new { Meta = "Data" })));

                Assert.Throws<InvalidOperationException>(() => voronStorage.Batch(mutator => mutator.Documents.PutDocumentMetadata("Foo2", new RavenJObject())));
            }
        }

        [Fact]
        public void DocumentStorage_DocumentAdd_DocumentByDifferentKey_NullReturned()
        {
            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator => mutator.Documents.AddDocument("Foo", null, RavenJObject.FromObject(new { Name = "Bar" }), RavenJObject.FromObject(new { Meta = "Data" })));

                JsonDocumentMetadata metadata = null;
                Assert.DoesNotThrow(
                    () => voronStorage.Batch(viewer => metadata = viewer.Documents.DocumentMetadataByKey("Foo2", null)));
                
                Assert.Equal(null,metadata);

            }
        }

        [Fact]
        public void DocumentStorage_DocumentAdd_MetadataUpdated_MetadataRead()
        {
            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator => mutator.Documents.AddDocument("Foo", null, RavenJObject.FromObject(new { Name = "Bar" }), RavenJObject.FromObject(new { Meta = "Data" })));

                voronStorage.Batch(mutator => mutator.Documents.AddDocument("Foo", null, RavenJObject.FromObject(new { Name = "Bar" }), RavenJObject.FromObject(new { Meta = "NotData" })));


                JsonDocumentMetadata metadata = null;
                voronStorage.Batch(viewer => metadata = viewer.Documents.DocumentMetadataByKey("Foo", null));
                Assert.Equal("NotData", metadata.Metadata.Value<string>("Meta"));

            }
        }

        [Fact]
        public void DocumentStorage_DocumentUpdate_WithSpecifiedEtag_And_DocumentRead()
        {
            using (var voronStorage = NewVoronStorage())
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

        [Fact]
        public void DocumentStorage_DocumentTouch_EtagUpdated()
        {
            const string TEST_DOCUMENT_KEY = "Foo";
            using (var voronStorage = NewVoronStorage())
            {
                AddDocumentResult initialAddResult = null;
                voronStorage.Batch(mutator => initialAddResult = mutator.Documents.AddDocument(TEST_DOCUMENT_KEY, Etag.Empty, RavenJObject.FromObject(new {Name = "Bar"}), new RavenJObject()));

                Etag preTouchEtag = null;
                Etag afterTouchEtag = null;
                voronStorage.Batch(mutator => mutator.Documents.TouchDocument(TEST_DOCUMENT_KEY, out preTouchEtag, out afterTouchEtag));

                Assert.NotNull(preTouchEtag);
                Assert.NotNull(afterTouchEtag);

                Assert.Equal(initialAddResult.Etag, preTouchEtag);
                Assert.NotEqual(preTouchEtag, afterTouchEtag);
            }
        }

	    [Fact]
	    public void DocumentStorage_GetDocumentsAfter_NoDocuments_EmptyCollectionReturned()
	    {
	        using (var voronStorage = NewVoronStorage())
	        {
                voronStorage.Batch(viewer => Assert.Empty(viewer.Documents.GetDocumentsAfter(Etag.Empty,25)));
	        }
	    }

	    [Fact]
        public void DocumentStorage_GetDocumentsAfter()
        {
            const int DOCUMENT_COUNT = 12;
            const int DOCUMENT_SKIP_COUNT = DOCUMENT_COUNT/4;

            using (var voronStorage = NewVoronStorage())
            {
                var documentAddResults = new List<AddDocumentResult>();
                voronStorage.Batch(mutator =>
                {
                    for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
                        documentAddResults.Add(mutator.Documents.InsertDocument("Foo" + docIndex,
                            RavenJObject.FromObject(new {Name = "Bar"}), new RavenJObject(), true));
                });

                documentAddResults = documentAddResults.OrderBy(row => row.Etag).ToList();

                var documentEtagsAfterSkip =
                    documentAddResults.Select(row => row.Etag).Skip(DOCUMENT_SKIP_COUNT).ToArray();

                IEnumerable<JsonDocument> fetchedDocuments = null;
                voronStorage.Batch(
                    viewer =>
                        fetchedDocuments =
                            viewer.Documents.GetDocumentsAfter(documentEtagsAfterSkip.First(),
                                documentEtagsAfterSkip.Length));

                var fetchedEtagList = fetchedDocuments.OrderBy(row => row.Etag)
                                                      .Select(row => row.Etag)
                                                      .ToList();

                Assert.Empty(fetchedEtagList.Except(documentEtagsAfterSkip.Skip(1)));
                Assert.Empty(documentEtagsAfterSkip.Skip(1).Except(fetchedEtagList));
            }
        }

        [Fact]
        public void DocumentStorage_GetDocumentsAfter_With_EtagUntil()
        {
            const int DOCUMENT_COUNT = 12;
            const int DOCUMENT_SKIP_COUNT = DOCUMENT_COUNT/4;

            using (var voronStorage = NewVoronStorage())
            {
                var documentAddResults = new List<AddDocumentResult>();
                voronStorage.Batch(mutator =>
                {
                    for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
                        documentAddResults.Add(mutator.Documents.InsertDocument("Foo" + docIndex,
                            RavenJObject.FromObject(new {Name = "Bar"}), new RavenJObject(), true));
                });

                documentAddResults = documentAddResults.OrderBy(row => row.Etag).ToList();

                var documentEtagsAfterSkip = documentAddResults.Select(row => row.Etag)
                    .Skip(DOCUMENT_SKIP_COUNT)
                    .Take(DOCUMENT_COUNT - (DOCUMENT_SKIP_COUNT * 2))
                    .ToArray();

                IEnumerable<JsonDocument> fetchedDocuments = null;
                voronStorage.Batch(
                    viewer =>
                        fetchedDocuments =
                            viewer.Documents.GetDocumentsAfter(documentEtagsAfterSkip.First(),
                                documentEtagsAfterSkip.Length, null, documentEtagsAfterSkip.Last()));

                var fetchedEtagList = fetchedDocuments.OrderBy(row => row.Etag)
                                                      .Select(row => row.Etag)
                                                      .ToList();

                Assert.Empty(fetchedEtagList.Except(documentEtagsAfterSkip.Skip(1)));
                Assert.Empty(documentEtagsAfterSkip.Skip(1).Except(fetchedEtagList));

            }
        }

        [Fact]
        public void DocumentStorage_GetDocumentsAfter_With_Take()
        {
            const int DOCUMENT_COUNT = 12;
            const int DOCUMENT_SKIP_COUNT = DOCUMENT_COUNT / 4;
            const int DOCUMENT_TAKE_VALUE = DOCUMENT_COUNT/3;

            using (var voronStorage = NewVoronStorage())
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

                IEnumerable<JsonDocument> fetchedDocuments = null;
                voronStorage.Batch(
                    viewer =>
                        fetchedDocuments =
                            viewer.Documents.GetDocumentsAfter(documentEtagsAfterSkip.First(),
                                DOCUMENT_TAKE_VALUE, null, documentEtagsAfterSkip.Last()));

                var fetchedEtagList = fetchedDocuments.OrderBy(row => row.Etag)
                                                      .Select(row => row.Etag)
                                                      .ToList();

                Assert.Empty(fetchedEtagList.Except(expectedDocumentSet));
                Assert.Empty(expectedDocumentSet.Except(fetchedEtagList));

            }
        }

        [Fact]
        public void DocumentStorage_GetDocumentsAfter_With_MaxSize()
        {
            const int DOCUMENT_COUNT = 12;
            const int DOCUMENT_SKIP_COUNT = DOCUMENT_COUNT/4;
            const int MAX_SIZE = 100;

            using (var voronStorage = NewVoronStorage())
            {
                voronStorage.Batch(mutator =>
                {
                    for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
                        mutator.Documents.InsertDocument("Foo" + docIndex,RavenJObject.FromObject(new {Name = "Bar"}), new RavenJObject(), true);
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
                    if(totalDocumentSizeSoFar >= MAX_SIZE)
                        break;                    
                }

                IEnumerable<JsonDocument> fetchedDocuments = null;
                voronStorage.Batch(
                    viewer =>
                        fetchedDocuments =
                            viewer.Documents.GetDocumentsAfter(documentAfterSkip.First().Etag,
                                documentAfterSkip.Length, MAX_SIZE, documentAfterSkip.Last().Etag));

                var fetchedEtagList = fetchedDocuments.OrderBy(row => row.Etag)
                                                       .Select(row => row.Etag)
                                                       .ToList();

                Assert.Empty(fetchedEtagList.Except(expectedDocumentSet.Select(row => row.Etag)));
                Assert.Empty(expectedDocumentSet.Select(row => row.Etag).Except(fetchedEtagList));

            }
        }

        [Fact]
        public void DocumentStorage_GetDocumentsByReverseUpdateOrder_RetrievedWithCorrectOrder()
        {
            const int DOCUMENT_COUNT = 12;

            using (var voronStorage = NewVoronStorage())
            {
                var documentAddResults = new List<AddDocumentResult>();
                voronStorage.Batch(mutator =>
                {
                    for (int docIndex = 0; docIndex < DOCUMENT_COUNT; docIndex++)
                        documentAddResults.Add(mutator.Documents.InsertDocument("Foo" + docIndex,
                            RavenJObject.FromObject(new {Name = "Bar"}), new RavenJObject(), true));
                });

                documentAddResults.Reverse();

                List<JsonDocument> documentReverseOrderFetchResults = null;
                voronStorage.Batch(viewer => documentReverseOrderFetchResults = viewer.Documents.GetDocumentsByReverseUpdateOrder(0, documentAddResults.Count).ToList());

                Assert.NotNull(documentReverseOrderFetchResults);
                
                for(var etagIndex = 0;etagIndex < documentAddResults.Count; etagIndex++)
                {
                    Assert.True(documentAddResults[etagIndex].Etag.Equals(documentReverseOrderFetchResults[etagIndex].Etag));
                }
            }
        }

	    [Fact]
	    public void DocumentStorage_GetDocumentsByReverseUpdateOrder_NoDocuments_EmptyCollectionReturned()
	    {
	        using (var voronStorage = NewVoronStorage())
	        {
                voronStorage.Batch(viewer =>
                {
                    var fetchedDocuments = viewer.Documents.GetDocumentsByReverseUpdateOrder(0,125);
                    Assert.Empty(fetchedDocuments);
                });
	        }
	    }

	    [Theory]
        [InlineData(15, 0)]
        [InlineData(15, 5)]
        [InlineData(5, 7)]
        public void DocumentStorage_GetDocumentsByReverseUpdateOrder_RetrievedWithCorrectOrder_DifferentSkipParameters(int itemCount, int skip)
        {
	        using (var voronStorage = NewVoronStorage())
            {
                var documentAddResults = new List<AddDocumentResult>();
                voronStorage.Batch(mutator =>
                {
                    for (int docIndex = 0; docIndex < itemCount; docIndex++)
// ReSharper disable once AccessToModifiedClosure
                        documentAddResults.Add(mutator.Documents.InsertDocument("Foo" + docIndex,
                            RavenJObject.FromObject(new { Name = "Bar" }), new RavenJObject(), true));
                });

                documentAddResults.Reverse();
                documentAddResults = documentAddResults.Skip(skip).ToList();

                List<JsonDocument> documentReverseOrderFetchResults = null;
                voronStorage.Batch(viewer => documentReverseOrderFetchResults = viewer.Documents.GetDocumentsByReverseUpdateOrder(skip, documentAddResults.Count).ToList());

                Assert.NotNull(documentReverseOrderFetchResults);

                if(skip >= itemCount)
                    Assert.Empty(documentReverseOrderFetchResults);

                for (var etagIndex = 0; etagIndex < documentAddResults.Count; etagIndex++)
                {
                    Assert.True(documentAddResults[etagIndex].Etag.Equals(documentReverseOrderFetchResults[etagIndex].Etag));
                }
            }
        }

        [Theory]
        [InlineData(25, 0, 20)]
        [InlineData(25, 5, 10)]
        [InlineData(15, 3, 6)]
        [InlineData(5, 0, 0)]
        public void DocumentStorage_GetDocumentsWithIdStartingWith(int itemCount, int start, int take)
        {
            using (var voronStorage = NewVoronStorage())
            {
                var inputData = new Dictionary<string, RavenJObject>();
                voronStorage.Batch(mutator =>
                    {
                        for (int itemIndex = 0; itemIndex < itemCount; itemIndex++)
                        {
                            var keyPrefix = (itemIndex%2 == 0) ? "Foo" : "Bar";
                            var document = RavenJObject.FromObject(new {Name = "Bar" + itemIndex});

                            mutator.Documents.AddDocument(keyPrefix + itemIndex, Etag.Empty, document, new RavenJObject());
                            inputData.Add(keyPrefix + itemIndex,document);
                        }
                    });

                IList<JsonDocument> fetchedDocuments = null;
                voronStorage.Batch(viewer => fetchedDocuments = viewer.Documents.GetDocumentsWithIdStartingWith("Bar", start, take).ToList());

                Assert.NotNull(fetchedDocuments);
                var relevantInputKeys = new HashSet<string>(inputData.Where(kvp => kvp.Key.StartsWith("Bar"))
                                                                                          .OrderBy(kvp => kvp.Key)
                                                                                          .Select(row => row.Key)
                                                                                          .Skip(start)
                                                                                          .Take(take));

                var fetchedDocumentKeys = new HashSet<string>(fetchedDocuments.Select(doc => doc.Key));

                Assert.True(fetchedDocumentKeys.Count <= take);
                Assert.True(fetchedDocumentKeys.SetEquals(relevantInputKeys));
            }
        }

	    [Fact]
	    public void DocumentStorage_GetDocumentsWithIdStartingWith_NoDocuments_EmptyCollectionReturned()
	    {
	        using (var voronStorage = NewVoronStorage())
	        {
                voronStorage.Batch(viewer => Assert.Empty(viewer.Documents.GetDocumentsWithIdStartingWith("Foo", 0, 25).ToList()));
            }
	    }

	    [Fact]
        public void DocumentStorage_GetDocumentsWithIdStartingWith_WithNonZeroStart()
        {
            using (var voronStorage = NewVoronStorage())
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

        [Fact]
        public void DocumentStorage_GetDocumentsWithIdStartingWith_WithTake()
        {
            using (var voronStorage = NewVoronStorage())
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
