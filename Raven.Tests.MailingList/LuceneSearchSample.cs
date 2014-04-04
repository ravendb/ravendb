using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    internal class LuceneSearchSample : RavenTestBase
    {
        [Fact]
        public void LuceneSearchTest()
        {
            using (var store = NewDocumentStore())
            {
                new SampleSearchIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new SampleModel
                    {
                        Id = "NotNull",
                        NullableInt = 1
                    });

                    session.Store(new SampleModel2
                    {
                        Id = "WithNull"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result1 = session.Advanced.DocumentQuery<SampleIndexView, SampleSearchIndex>().WhereEquals(p => p.NullableInt, (int?)1).WaitForNonStaleResultsAsOfNow().ToList();
                    Assert.Equal(result1.Count, 1);

                    var result2 = session.Advanced.DocumentQuery<SampleIndexView, SampleSearchIndex>().WhereGreaterThan(p => p.NullableInt, (int?)0).WaitForNonStaleResultsAsOfNow().ToList();
                    Assert.Equal(result2.Count, 1);

                    var result3 = session.Advanced.DocumentQuery<SampleIndexView, SampleSearchIndex>().WhereGreaterThanOrEqual(p => p.NullableInt, (int?)1).WaitForNonStaleResultsAsOfNow().ToList();
                    Assert.Equal(result3.Count, 1);

                    var result4 = session.Advanced.DocumentQuery<SampleIndexView, SampleSearchIndex>().WhereBetween(p => p.NullableInt, (int?)0, (int?)2).WaitForNonStaleResultsAsOfNow().ToList();
                    Assert.Equal(result4.Count, 1);

                    var result5 = session.Advanced.DocumentQuery<SampleIndexView, SampleSearchIndex>().WhereBetweenOrEqual(p => p.NullableInt, (int?)1, (int?)1).WaitForNonStaleResultsAsOfNow().ToList();
                    Assert.Equal(result5.Count, 1);

                    var result6 = session.Advanced.DocumentQuery<SampleIndexView, SampleSearchIndex>().WhereLessThan(p => p.NullableInt, (int?)2).WaitForNonStaleResultsAsOfNow().ToList();
                    Assert.Equal(result6.Count, 1);

                    var result7 = session.Advanced.DocumentQuery<SampleIndexView, SampleSearchIndex>().WhereLessThanOrEqual(p => p.NullableInt, (int?)1).WaitForNonStaleResultsAsOfNow().ToList();
                    Assert.Equal(result7.Count, 1);
                }
            }
        }

        public class SampleModel
        {
            public string Id { get; set; }
            public string IrrelevantData { get; set; }
            public int? NullableInt { get; set; }
        }

        public class SampleModel2
        {
            public string Id { get; set; }
            public string IrrelevantData { get; set; }
        }

        public class SampleIndexView
        {
            public string Id { get; set; }
            public int? NullableInt { get; set; }
        }

        public class SampleSearchIndex : AbstractMultiMapIndexCreationTask<SampleIndexView>
        {
            public SampleSearchIndex()
            {

                AddMap<SampleModel2>(sampleModels => from sm in sampleModels
                                                     select new
                                                     {
                                                         Id = sm.Id,
                                                         NullableInt = (int?)null
                                                     });

                AddMap<SampleModel>(sampleModels => from sm in sampleModels
                                                    select new
                                                    {
                                                        Id = sm.Id,
                                                        NullableInt = sm.NullableInt
                                                    });

                Reduce = results => from result in results
                                    group result by new {result.Id}
                                    into gr
                                    select new
                                    {
                                        Id = gr.Key.Id,
                                        NullableInt = gr.Select(x => x.NullableInt).OrderBy(x => x).FirstOrDefault(x => x != null)
                                    };
            }
        }
    }
}
