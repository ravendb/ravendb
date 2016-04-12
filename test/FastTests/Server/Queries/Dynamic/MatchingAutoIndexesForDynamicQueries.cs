using System;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries.Dynamic;

using Xunit;

namespace FastTests.Server.Queries.Dynamic
{
    public class MatchingAutoIndexesForDynamicQueries : RavenLowLevelTestBase
    {
        private readonly DocumentDatabase _documentDatabase;
        private readonly DynamicQueryToIndexMatcher _sut;

        public MatchingAutoIndexesForDynamicQueries()
        {
            _documentDatabase = CreateDocumentDatabase();

            _sut = new DynamicQueryToIndexMatcher(_documentDatabase.IndexStore);
        }

        [Fact]
        public void Failure_if_there_is_no_index()
        {
            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "Name:Arek" });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Failure_if_there_is_no_index_for_given_collection()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Companies", new IndexQuery { Query = "Name:IBM" });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Complete_match_for_single_matching_index()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "Name:Arek" });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_for_index_containing_all_fields()
        {
            var usersByName = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var usersByNameAndAge = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
                new IndexField
                {
                    Name = "Age",
                    Highlighted = false,
                    Storage = FieldStorage.No
                }
            });

            add_index(usersByName);
            add_index(usersByNameAndAge);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "Name:Arek Age:29" });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(usersByNameAndAge.Name, result.IndexName);
        }

        [Fact]
        public void PartialMatch_for_index_containing_only_part_of_indexes_fields()
        {
            var usersByName = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            add_index(usersByName);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "Name:Arek Age:29" });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            Assert.Equal(usersByName.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_for_single_matching_index_with_mapping_nested_fields()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
                new IndexField
                {
                    Name = "Address.Street",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
                new IndexField
                {
                    Name = "Friends,Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "Name:Arek AND Address.Street:1stAvenue AND Friends,Name:Jon" });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_for_single_matching_index_with_simple_sort_option()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Name:Arek",
                SortedFields = new[] { new SortedField("Name") },
            });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Failure_when_sort_options_do_not_match()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Weight",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Weight:70",
                SortedFields = new[] { new SortedField("Weight_Range") },
            });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Partial_match_when_sort_field_is_not_mapped()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Name:Arek",
                SortedFields = new[] { new SortedField("Weight") },
            });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_query_sort_is_default_and_definition_doesn_not_specify_sorting_at_all()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Age",
                    Highlighted = false,
                    Storage = FieldStorage.No,
                    SortOption = SortOptions.NumericDefault
                },
            });

            add_index(definition);

            var dynamicQueryWithStringSorting = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Age_Range:{Lx9 TO NULL}",
                SortedFields = new[] { new SortedField("Age_Range") },
            });

            var result = _sut.Match(dynamicQueryWithStringSorting);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);

            var dynamicQueryWithNoneSorting = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Age_Range:31",
                SortedFields = new[] { new SortedField("Age_Range") },
            });

            result = _sut.Match(dynamicQueryWithNoneSorting);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Failure_if_matching_index_is_disabled_errored_or_has_lot_of_errors()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "Name:Arek" });

            var index = get_index(definition.Name);

            index.SetPriority(IndexingPriority.Disabled);

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);

            index.SetPriority(IndexingPriority.Error);

            result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);

            index.SetPriority(IndexingPriority.Normal);
            index._indexStorage.UpdateStats(DateTime.UtcNow, new IndexingBatchStats()
            {
                IndexingAttempts = 1000,
                IndexingErrors = 900
            });

            result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        private void add_index(AutoMapIndexDefinition definition)
        {
            _documentDatabase.IndexStore.CreateIndex(definition);
        }

        private Index get_index(string name)
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