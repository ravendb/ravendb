using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace FastTests.Server.Documents.Queries.Dynamic.Map
{
    public class MatchingAutoMapIndexesForDynamicQueries : RavenLowLevelTestBase
    {
        private readonly DocumentDatabase _documentDatabase;
        private readonly DynamicQueryToIndexMatcher _sut;

        public MatchingAutoMapIndexesForDynamicQueries()
        {
            _documentDatabase = CreateDocumentDatabase();

            _sut = new DynamicQueryToIndexMatcher(_documentDatabase.IndexStore);
        }

        [Fact]
        public void Failure_if_there_is_no_index()
        {
            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek'"));

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
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Companies WHERE Name = 'IBM'"));

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
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek'"));

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
                    Storage = FieldStorage.No
                },
            });

            var usersByNameAndAge = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
                new IndexField
                {
                    Name = "Age",
                    Storage = FieldStorage.No,
                    Sort = SortOptions.Numeric
                }
            });

            add_index(usersByName);
            add_index(usersByNameAndAge);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' AND Age = 29"));

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
                    Storage = FieldStorage.No
                },
            });

            add_index(usersByName);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' AND Age = 29"));

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
                    Storage = FieldStorage.No
                },
                new IndexField
                {
                    Name = "Address.Street",
                    Storage = FieldStorage.No
                },
                new IndexField
                {
                    Name = "Friends,Name",
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);
            
            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' AND Address.Street ='1stAvenue' AND Friends[].Name = 'Jon'"));

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_for_single_matching_index_with_default_string_sort_option()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' ORDER BY Name"));

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_for_single_matching_index_with_numeric_sort_option_for_nested_field()
        {
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new IndexField
                {
                    Name = "Address.ZipCode",
                    Storage = FieldStorage.No,
                    Sort = SortOptions.Numeric
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users ORDER BY Address.ZipCode AS double"));

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
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Weigth = 70 ORDER BY Weight AS double"));

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
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' ORDER BY Weight"));

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
                    Storage = FieldStorage.No,
                    Sort = SortOptions.Numeric
                },
            });

            add_index(definition);

            var dynamicQueryWithStringSorting = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Age > 9 ORDER BY Age AS long"));

            var result = _sut.Match(dynamicQueryWithStringSorting);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);

            var dynamicQueryWithNoneSorting = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Age = 31 ORDER BY Age AS double"));

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
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek'"));

            var index = get_index(definition.Name);

            index.SetState(IndexState.Disabled);

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);

            index.SetState(IndexState.Error);

            result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);

            index.SetPriority(IndexPriority.Normal);
            index._indexStorage.UpdateStats(DateTime.UtcNow, new IndexingRunStats()
            {
                MapAttempts = 1000,
                MapErrors = 900
            });

            result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        private void add_index(AutoMapIndexDefinition definition)
        {
            AsyncHelpers.RunSync(() => _documentDatabase.IndexStore.CreateIndex(definition));
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