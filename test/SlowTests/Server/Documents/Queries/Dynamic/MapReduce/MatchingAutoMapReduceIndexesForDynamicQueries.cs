using System.Runtime.CompilerServices;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace SlowTests.Server.Documents.Queries.Dynamic.MapReduce
{
    public class MatchingAutoMapReduceIndexesForDynamicQueries : RavenLowLevelTestBase
    {
        private DocumentDatabase _documentDatabase;
        protected DynamicQueryToIndexMatcher _sut;

        public void Initialize([CallerMemberName] string caller = null)
        {
            _documentDatabase = CreateDocumentDatabase(caller: $"{GetType().Name}.{caller}");

            _sut = new DynamicQueryToIndexMatcher(_documentDatabase.IndexStore);
        }

        [Fact]
        public void Failure_match_if_there_is_no_index()
        {
            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users GROUP BY Location SELECT Location, count() "));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Complete_match_for_single_matching_index_with_sort_options()
        {
            var definition = new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Count,
                },
            },
            new[]
            {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide(
                "FROM Users GROUP BY Location WHERE Location = 'Poland' ORDER BY Count AS long ASC, Location ASC SELECT Location, count() "));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Failure_match_for_map_index_containing_the_same_field_names()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                },
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users GROUP BY Location WHERE Location = 'Poland' SELECT Location, count() "));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Failure_match_for_map_reduce_index_having_different_aggregation_function()
        {
            var definition = new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Count
                },
            },
            new[]
            {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users GROUP BY Location SELECT Location, sum(Count) "));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Partial_match_for_map_reduce_index_not_having_all_map_fields_defined_in_query()
        {
            var definition = new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Count,
                },
            },
            new[]
            {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users GROUP BY Location SELECT Location, count(), sum(Sum) "));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Failure_match_for_map_reduce_index_not_matching_exactly_group_by_fields()
        {
            // missing nick name field to match
            var usersByCountReducedByAgeAndLocation = new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Count
                },
            },
            new[]
            {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                },
                new AutoIndexField
                {
                    Name = "Age",
                    Storage = FieldStorage.Yes,
                }
            });

            // additional Age field to match
            var usersByCountReducedByLocationAndNickNameAndAge = new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Count
                },
            },
            new[]
            {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                },
                new AutoIndexField
                {
                    Name = "NickName",
                    Storage = FieldStorage.Yes,
                },
                new AutoIndexField
                {
                    Name = "Age",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(usersByCountReducedByAgeAndLocation);
            add_index(usersByCountReducedByLocationAndNickNameAndAge);

            var dynamicQuery =
                DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users GROUP BY Location, NickName WHERE Location = 'Poland' SELECT Location, count() "));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Complete_match_and_surpassed_map_reduce_index_is_choosen()
        {
            var usersByCountGroupedByLocation = new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Count,
                },
            },
            new[]
            {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            var usersByCountAndTotalAgeGroupedByLocation = new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Count,
                },
                new AutoIndexField
                {
                    Name = "TotalAge",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Sum
                },
            },
            new[]
            {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(usersByCountGroupedByLocation);
            add_index(usersByCountAndTotalAgeGroupedByLocation);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users GROUP BY Location WHERE Location = 'Poland' SELECT Location, count() "));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(usersByCountAndTotalAgeGroupedByLocation.Name, result.IndexName);
        }

        [Fact]
        public void Partial_match_when_sort_field_is_not_mapped()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' ORDER BY Weight ASC"));
 
            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Partial_match_if_analyzer_is_required_on_group_by_field()
        {
            using (var db = CreateDocumentDatabase())
            {
                var mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"
from Users
group by Name
where Name = 'arek'
select Name, count()"));

                db.IndexStore.CreateIndex(mapping.CreateAutoIndexDefinition()).Wait();

                mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"
from Users
group by Name
where search(Name, 'arek')
select Name, count()"));

                var matcher = new DynamicQueryToIndexMatcher(db.IndexStore);

                var result = matcher.Match(mapping, null);

                Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            }
        }

        [Fact]
        public void Partial_match_if_exact_is_required_on_group_by_field()
        {
            using (var db = CreateDocumentDatabase())
            {
                var mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"
from Users
group by Name
where Name = 'arek'
select Name, count()"));

                db.IndexStore.CreateIndex(mapping.CreateAutoIndexDefinition()).Wait();

                mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"
from Users
group by Name
where exact(Name = 'arek')
select Name, count()"));

                var matcher = new DynamicQueryToIndexMatcher(db.IndexStore);

                var result = matcher.Match(mapping, null);

                Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            }
        }

        protected void add_index(IndexDefinitionBase definition)
        {
            AsyncHelpers.RunSync(() => _documentDatabase.IndexStore.CreateIndex(definition));
        }

        protected Index get_index(string name)
        {
            return _documentDatabase.IndexStore.GetIndex(name);
        }

        public override void Dispose()
        {
            try
            {
                _documentDatabase.Dispose();
            }
            finally
            {
                base.Dispose();
            }
        }
    }
}
