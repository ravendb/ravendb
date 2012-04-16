using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Bundles.MoreLikeThis;
using Raven.Client.Indexes;
using Raven.Client.MoreLikeThis;
using Xunit;
using MoreLikeThisQueryParameters = Raven.Client.MoreLikeThis.MoreLikeThisQueryParameters;


namespace Raven.Bundles.Tests.MoreLikeThis
{
    public class MoreLikeThis_should_support_MapReduce_indexes : TestWithInMemoryDatabase
    {
        public string DolanId;

        public MoreLikeThis_should_support_MapReduce_indexes() : 
            base(config =>
        {
            config.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(MoreLikeThisResponder).Assembly));
        })
        {
            using(var session = documentStore.OpenSession())
            {
                var dolan = new Thing()
                {
                    Name = "Dolan"
                };
                session.Store(dolan);

                session.SaveChanges();

                DolanId = dolan.Id;

                Assert.NotNull(DolanId);

                session.Store(new Opinion()
                {
                    TargetId = DolanId,
                    Value = "cannot be trusted"
                });

                session.SaveChanges();
            }
        }

        [Fact]
        public void Can_find_document_related_by_name()
        {
            new MapReduceIndex().Execute(documentStore);

            using (var session = documentStore.OpenSession())
            {
                var person = new Thing()
                {
                    Name = "Cousin of Dolan"
                };
                session.Store(person);
                session.SaveChanges();

                var results = session.Query<IndexDocument, MapReduceIndex>().Customize(x => x.WaitForNonStaleResults()).Count();

                Assert.Equal(results, 2);

                Assert.Empty(documentStore.DatabaseCommands.GetStatistics().Errors);
            }

            using (var session = documentStore.OpenSession())
            {
                var list = session.Advanced.MoreLikeThis<IndexDocument, MapReduceIndex>(
                    new MoreLikeThisQueryParameters
                {
//                    ReduceKey = DolanId,
                    MinimumDocumentFrequency = 1
                });

                Assert.Equal(1, list.Count());
            }
        }


        public class Thing
        {
            public string Id { get; set; }
            public string Name;
        }

        public class Opinion
        {
            public string TargetId;
            public string Value;
        }

        public class IndexDocument
        {
            public string TargetId;
            public string Text;
        }


        public class MapReduceIndex : AbstractMultiMapIndexCreationTask<IndexDocument>
        {
            public override string IndexName
            {
                get
                {
                    return "MapReduceIndex";
                }
            }

            public MapReduceIndex()
            {
                this.AddMap<Thing>(things => from thing in things select new IndexDocument()
                {
                    TargetId = thing.Id,
                    Text = thing.Name
                });

                this.AddMap<Opinion>(opinions => from opinion in opinions select new IndexDocument()
                {
                    TargetId = opinion.TargetId,
                    Text = opinion.Value
                });

                this.Reduce = documents => from doc in documents
                                           group doc by doc.TargetId into g
                                           select new IndexDocument()
                                           {
                                               TargetId = g.Key,
                                               Text = string.Join(" ", g.Select(d => d.Text).ToArray<string>())
                                           };


                Analyzers = new Dictionary<Expression<Func<IndexDocument, object>>, string>
				{
					{ x => x.Text, typeof (StandardAnalyzer).FullName }, 
                    { x => x.TargetId, typeof(KeywordAnalyzer).FullName }
				};
            }
        }
    }
}
