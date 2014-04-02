// -----------------------------------------------------------------------
//  <copyright file="MapReduceListener.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Xunit;

namespace Raven.Tests.Views
{
    public class MapReduceListener : RavenTest
    {
        private EmbeddableDocumentStore store;

        class Trigger : AbstractIndexUpdateTrigger
        {
            public override AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName)
            {
                return new TriggerBatcher(indexName, Database);
            }
        }
        class TriggerBatcher : AbstractIndexUpdateTriggerBatcher
        {
            private readonly string indexName;
            private readonly DocumentDatabase database;
            public static bool CurrentIndexingScopeIsNull;

            public TriggerBatcher(string indexName, DocumentDatabase database)
            {
                this.indexName = indexName;
                this.database = database;

            }

            public override void OnIndexEntryCreated(string entryKey, Lucene.Net.Documents.Document document)
            {
                CurrentIndexingScopeIsNull = false;
                if (CurrentIndexingScope.Current != null)
                {
                    document.Add(new Field("TestField", "testvalue", Field.Store.NO, Field.Index.NOT_ANALYZED));
                }
                else
                {
                    CurrentIndexingScopeIsNull = true;
                }
            }
        }

        class QueryListener : AbstractIndexQueryTrigger
        {
            public override Query ProcessQuery(string indexName, Query query, IndexQuery originalQuery)
            {
                var allowedQuery = new BooleanQuery
                {
                    new BooleanClause(new TermQuery(new Term("TestField", "testvalue")), Occur.MUST)
                };

                return new FilteredQuery(query, new QueryWrapperFilter(allowedQuery));
            }
        }
        public MapReduceListener()
        {
            store = NewDocumentStore(catalog: new TypeCatalog(typeof(Trigger), typeof(QueryListener)));
        }

        [Fact]
        public void WhenMapIndexIsCaughtByAListener_CanAddFieldsToIndexedDocuments()
        {
            new SampleItem_SimpleMap().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new SampleItem {Title="Some title"});
                session.SaveChanges();

                var results = session.Advanced.LuceneQuery<SampleItem, SampleItem_SimpleMap>()
                    .WaitForNonStaleResults()
                    .ToList();
                Assert.False(TriggerBatcher.CurrentIndexingScopeIsNull);
                Assert.NotEmpty(results);
                Assert.NotNull(results.FirstOrDefault(r => r.Title == "Some title"));
            }
        }       
        
        [Fact]
        public void WhenMapReduceIndexIsCaughtByAListener_CanAddFieldsToIndexedDocuments()
        {
            new SampleItem_MapReduce().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new SampleItem {Title="Some title"});
                session.SaveChanges();

                var results = session.Advanced.LuceneQuery<SampleItem_MapReduce.Result, SampleItem_MapReduce>()
                    .WaitForNonStaleResults()
                    .ToList();
                Assert.False(TriggerBatcher.CurrentIndexingScopeIsNull);
                Assert.NotEmpty(results);
                Assert.NotNull(results.FirstOrDefault(r => r.Title == "Some title"));
            }
        }

        class SampleItem_SimpleMap : AbstractIndexCreationTask<SampleItem>
        {
            public SampleItem_SimpleMap()
            {
                Map = items => from item in items
                               select new
                               {
                                   item.Title,
                                   item.SecurityDocumentId,
                               };
            }
        }

        class SampleItem_MapReduce : AbstractIndexCreationTask<SampleItem, SampleItem_MapReduce.Result>
        {
            internal class Result
            {
                public string Title { get; set; }
                public int Count { get; set; }
            }
            public SampleItem_MapReduce()
            {
                Map = items => from item in items
                               select new
                               {
                                   item.Title,
                                   Count = 1
                               };
                Reduce = items => from item in items
                                  group item by item.Title 
                                  into g
                                  select new
                                  {
                                      Title = g.Key,
                                      Count = g.Sum(i => i.Count)
                                  };
            }
        }

        public class SampleItem : ISecuredType
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public string SecurityDocumentId { get; set; }
        }
        public interface ISecuredType
        {
            string SecurityDocumentId { get; set; }
        }
    }
}