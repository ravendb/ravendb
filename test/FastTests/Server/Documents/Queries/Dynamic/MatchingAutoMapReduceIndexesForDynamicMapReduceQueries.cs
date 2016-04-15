using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace FastTests.Server.Documents.Queries.Dynamic
{
    public class MatchingAutoMapReduceIndexesForDynamicMapReduceQueries : RavenLowLevelTestBase
    {
        private readonly DocumentDatabase _documentDatabase;
        private readonly DynamicQueryToIndexMatcher _sut;

        public MatchingAutoMapReduceIndexesForDynamicMapReduceQueries()
        {
            _documentDatabase = CreateDocumentDatabase();

            _sut = new DynamicQueryToIndexMatcher(_documentDatabase.IndexStore);
        }

        [Fact]
        public void Failure_match_if_there_is_no_index()
        {
            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "", DynamicMapReduceFields = new []
            {
                new DynamicMapReduceField
                {
                    Name = "Location",
                    IsGroupBy = true
                },
                new DynamicMapReduceField
                {
                    Name = "Count",
                    OperationType = FieldMapReduceOperation.Count
                }
            }});

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }


        [Fact]
        public void Complete_match_for_single_matching_index()
        {
            var definition = new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    MapReduceOperation = FieldMapReduceOperation.Count
                },
            },
            new[]
            {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Location:Poland",
                DynamicMapReduceFields = new[]
                {
                    new DynamicMapReduceField
                    {
                        Name = "Count",
                        OperationType = FieldMapReduceOperation.Count
                    },
                    new DynamicMapReduceField
                    {
                        Name = "Location",
                        IsGroupBy = true
                    }
                }
            });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Failure_match_for_map_index_containing_the_same_field_names()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                },
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "Location:Poland", DynamicMapReduceFields = new []
            {
                new DynamicMapReduceField
                {
                    Name = "Count",
                    OperationType = FieldMapReduceOperation.Count
                },
                new DynamicMapReduceField
                {
                    Name = "Location",
                    IsGroupBy = true
                }
            }});

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Failure_match_for_map_reduce_index_having_different_aggregation_function()
        {
            var definition = new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    MapReduceOperation = FieldMapReduceOperation.Count
                },
            },
            new[]
            {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Location:Poland",
                DynamicMapReduceFields = new[]
                {
                    new DynamicMapReduceField
                    {
                        Name = "Count",
                        OperationType = FieldMapReduceOperation.Sum
                    },
                    new DynamicMapReduceField
                    {
                        Name = "Location",
                        IsGroupBy = true
                    }
                }
            });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Partial_match_for_map_reduce_index_not_having_all_map_fields_defined_in_query()
        {
            var definition = new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    MapReduceOperation = FieldMapReduceOperation.Count
                },
            },
            new[]
            {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Location:Poland",
                DynamicMapReduceFields = new[]
                {
                    new DynamicMapReduceField
                    {
                        Name = "Count",
                        OperationType = FieldMapReduceOperation.Count
                    },
                    new DynamicMapReduceField
                    {
                        Name = "Sum",
                        OperationType = FieldMapReduceOperation.Sum
                    },
                    new DynamicMapReduceField
                    {
                        Name = "Location",
                        IsGroupBy = true
                    }
                }
            });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Failure_match_for_map_reduce_index_not_matching_exactly_group_by_fields()
        {
            // missing nick name field to match
            var usersByCountReducedByAgeAndLocation = new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    MapReduceOperation = FieldMapReduceOperation.Count
                },
            },
            new[]
            {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                },
                new IndexField
                {
                    Name = "Age",
                    Storage = FieldStorage.Yes,
                }
            });

            // additional Age field to match
            var usersByCountReducedByLocationAndNickNameAndAge = new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    MapReduceOperation = FieldMapReduceOperation.Count
                },
            },
            new[]
            {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                },
                new IndexField
                {
                    Name = "NickName",
                    Storage = FieldStorage.Yes,
                },
                new IndexField
                {
                    Name = "Age",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(usersByCountReducedByAgeAndLocation);
            add_index(usersByCountReducedByLocationAndNickNameAndAge);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Location:Poland",
                DynamicMapReduceFields = new[]
                {
                    new DynamicMapReduceField
                    {
                        Name = "Count",
                        OperationType = FieldMapReduceOperation.Count
                    },
                    new DynamicMapReduceField
                    {
                        Name = "Location",
                        IsGroupBy = true
                    },
                    new DynamicMapReduceField
                    {
                        Name = "NickName",
                        IsGroupBy = true
                    }
                }
            });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Complete_match_and_surpassed_map_reduce_index_is_choosen()
        {
            var usersByCountGroupedByLocation = new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    MapReduceOperation = FieldMapReduceOperation.Count
                },
            },
            new[]
            {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            var usersByCountAndTotalAgeGroupedByLocation = new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    MapReduceOperation = FieldMapReduceOperation.Count
                },
                new IndexField
                {
                    Name = "TotalAge",
                    Storage = FieldStorage.Yes,
                    MapReduceOperation = FieldMapReduceOperation.Sum
                },
            },
            new[]
            {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
            });

            add_index(usersByCountGroupedByLocation);
            add_index(usersByCountAndTotalAgeGroupedByLocation);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Location:Poland",
                DynamicMapReduceFields = new[]
                {
                    new DynamicMapReduceField
                    {
                        Name = "Count",
                        OperationType = FieldMapReduceOperation.Count
                    },
                    new DynamicMapReduceField
                    {
                        Name = "Location",
                        IsGroupBy = true
                    }
                }
            });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(usersByCountAndTotalAgeGroupedByLocation.Name, result.IndexName);
        }

        private void add_index(IndexDefinitionBase definition)
        {
            _documentDatabase.IndexStore.CreateIndex(definition);
        }
    }
}