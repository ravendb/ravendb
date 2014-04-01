// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1560.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1560 : RavenTest
	{
		class Developer
		{
			public string Nick { get; set; }
			public int Id { get; set; }
		}

		class DevelopersIndex : AbstractIndexCreationTask<Developer>
		{
			public DevelopersIndex()
			{
				Map = docs => docs.Where(d => d.Nick.StartsWith("M")).Select(d => new { d.Nick });
			}
		}

		[Fact]
		public void MetadataOnlyShouldWorkForIndexes_Remote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				MetadataOnlyShouldWorkForQuery(store);
			}
		}

        [Fact(Skip = "MetadataOnly is not supported for embedded")]
        public void MetadataOnlyShouldWorkForIndexes_Embedded()
        {
            using (var store = NewDocumentStore())
            {
                MetadataOnlyShouldWorkForQuery(store);
            }
        }

        [Fact]
        public void MetadataOnlyShouldWorkForStartsWith_Remote()
        {
            using (var store = NewRemoteDocumentStore())
            {
                MetadataOnlyShouldWorkForStartsWith(store);
            }
        }

        [Fact(Skip = "MetadataOnly is not supported for embedded")]
        public void MetadataOnlyShouldWorkForStartsWith_Embedded()
        {
            using (var store = NewDocumentStore())
            {
                MetadataOnlyShouldWorkForStartsWith(store);
            }
        }

        [Fact]
        public void MetadataOnlyShouldWorkForGetDocuments_Remote()
        {
            using (var store = NewRemoteDocumentStore())
            {
                MetadataOnlyShouldWorkForGetDocuments(store);
            }
        }

        [Fact(Skip = "MetadataOnly is not supported for embedded")]
        public void MetadataOnlyShouldWorkForGetDocuments_Embedded()
        {
            using (var store = NewDocumentStore())
            {
                MetadataOnlyShouldWorkForGetDocuments(store);
            }
        }

        [Fact]
        public void MetadataOnlyShouldWorkForGet_Remote()
        {
            using (var store = NewRemoteDocumentStore())
            {
                MetadataOnlyShouldWorkForGet(store);
            }
        }

        [Fact(Skip = "MetadataOnly is not supported for embedded")]
        public void MetadataOnlyShouldWorkForGet_Embedded()
        {
            using (var store = NewDocumentStore())
            {
                MetadataOnlyShouldWorkForGet(store);
            }
        }

        private static void CheckJsonDocuments(JsonDocument[] documents, bool expectMetadata)
        {
            documents = documents.Where(d => d.Key.StartsWith("developer")).ToArray();
            Assert.Equal(2, documents.Count());
            foreach (var developer in documents)
            {
                Assert.Equal(expectMetadata, developer.DataAsJson.ContainsKey("Nick"));
                Assert.True(developer.Metadata.ContainsKey("Raven-Entity-Name"));
            }
        }

        private static void MetadataOnlyShouldWorkForGet(IDocumentStore store)
        {
            PrepareAndIndex(store);

            var ids = store.DatabaseCommands.GetDocuments(0, 32)
                .Where(d => d.DataAsJson.ContainsKey("Nick") && d.DataAsJson["Nick"].Value<string>() == "Marcin")
                .Select(d => d.Key).ToArray();

            // query for metadata + object
            var withObjects = store.DatabaseCommands.Get(ids, null);
            Assert.Equal(1, withObjects.Results.Count);
            Assert.Equal("Marcin", withObjects.Results[0]["Nick"]);

            // query for metadata only
            var metadataOnly = store.DatabaseCommands.Get(ids, null, metadataOnly:true);
            Assert.Equal(1, metadataOnly.Results.Count);
            Assert.True(metadataOnly.Results[0].ContainsKey("@metadata"));
            Assert.False(metadataOnly.Results[0].ContainsKey("Nick"));
        }

	    private static void MetadataOnlyShouldWorkForGetDocuments(IDocumentStore store)
	    {
            PrepareAndIndex(store);

            // query for metadata + object
	        var withObjects = store.DatabaseCommands.GetDocuments(0, 32);
            CheckJsonDocuments(withObjects, true);

            // query for metadata only
	        var metadataOnly = store.DatabaseCommands.GetDocuments(0, 32, true);
            CheckJsonDocuments(metadataOnly, false);
	    }

	    private static void MetadataOnlyShouldWorkForStartsWith(IDocumentStore store)
	    {
	        PrepareAndIndex(store);

	        // query for metadata + object
	        var withObjects = store.DatabaseCommands.StartsWith("develop", null, 0, 32);
	        CheckJsonDocuments(withObjects, true);

	        // query for metadata only
	        var metadataOnly = store.DatabaseCommands.StartsWith("develop", null, 0, 32, metadataOnly: true);
	        CheckJsonDocuments(metadataOnly, false);
	    }

	    private static void MetadataOnlyShouldWorkForQuery(IDocumentStore store)
	    {
	        PrepareAndIndex(store);

	        //query for metadata + object
	        var withObjects = store.DatabaseCommands.Query(new DevelopersIndex().IndexName, new IndexQuery(), null);
	        Assert.False(withObjects.IsStale);
	        Assert.Equal(1, withObjects.Results.Count);
	        Assert.Equal("Marcin", withObjects.Results[0]["Nick"]);

	        // now query for metadata only
	        var metadataOnly = store.DatabaseCommands.Query(new DevelopersIndex().IndexName, new IndexQuery(), null,
	                                                        metadataOnly: true);
	        Assert.Equal(1, metadataOnly.Results.Count);
	        Assert.True(metadataOnly.Results[0].ContainsKey("@metadata"));
	        Assert.False(metadataOnly.Results[0].ContainsKey("Nick"));
	    }

	    private static void PrepareAndIndex(IDocumentStore store)
	    {
	        new DevelopersIndex().Execute(store);
	        using (var session = store.OpenSession())
	        {
	            session.Store(new Developer {Nick = "John"});
	            session.Store(new Developer {Nick = "Marcin"});
	            session.SaveChanges();
	        }

	        WaitForIndexing(store);
	    }

	}
}