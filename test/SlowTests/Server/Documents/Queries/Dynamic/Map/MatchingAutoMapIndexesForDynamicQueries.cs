using System;
using System.Runtime.CompilerServices;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace SlowTests.Server.Documents.Queries.Dynamic.Map
{
    public class MatchingAutoMapIndexesForDynamicQueries : RavenLowLevelTestBase
    {
        private DocumentDatabase _documentDatabase;
        private DynamicQueryToIndexMatcher _sut;

        public void Initialize([CallerMemberName] string caller = null)
        {
            _documentDatabase = CreateDocumentDatabase(caller: $"{GetType().Name}.{caller}");

            _sut = new DynamicQueryToIndexMatcher(_documentDatabase.IndexStore);
        }

        [Fact]
        public void Failure_if_there_is_no_index()
        {
            Initialize();
            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek'"));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Failure_if_there_is_no_index_for_given_collection()
        {
            Initialize();
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Companies WHERE Name = 'IBM'"));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Complete_match_for_single_matching_index()
        {
            Initialize();
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek'"));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_for_index_containing_all_fields()
        {
            Initialize();
            var usersByName = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
            });

            var usersByNameAndAge = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
                new AutoIndexField
                {
                    Name = "Age",
                    Storage = FieldStorage.No,
                }
            });

            add_index(usersByName);
            add_index(usersByNameAndAge);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' AND Age = 29"));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(usersByNameAndAge.Name, result.IndexName);
        }

        [Fact]
        public void PartialMatch_for_index_containing_only_part_of_indexes_fields()
        {
            Initialize();
            var usersByName = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
            });

            add_index(usersByName);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' AND Age = 29"));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            Assert.Equal(usersByName.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_for_single_matching_index_with_mapping_nested_fields()
        {
            Initialize();
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
                new AutoIndexField
                {
                    Name = "Address.Street",
                    Storage = FieldStorage.No
                },
                new AutoIndexField
                {
                    Name = "Friends[].Name",
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);
            
            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' AND Address.Street ='1stAvenue' AND Friends[].Name = 'Jon'"));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_for_single_matching_index_with_default_string_sort_option()
        {
            Initialize();
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' ORDER BY Name"));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_for_single_matching_index_with_numeric_sort_option_for_nested_field()
        {
            Initialize();
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Address.ZipCode",
                    Storage = FieldStorage.No,
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users ORDER BY Address.ZipCode AS double"));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Partial_match_when_sort_field_is_not_mapped()
        {
            Initialize();
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek' ORDER BY Weight"));

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Complete_match_query_sort_is_default_and_definition_doesn_not_specify_sorting_at_all()
        {
            Initialize();
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Age",
                    Storage = FieldStorage.No,
                },
            });

            add_index(definition);

            var dynamicQueryWithStringSorting = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Age > 9 ORDER BY Age AS long"));

            var result = _sut.Match(dynamicQueryWithStringSorting, null);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);

            var dynamicQueryWithNoneSorting = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Age = 31 ORDER BY Age AS double"));

            result = _sut.Match(dynamicQueryWithNoneSorting, null);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void Failure_if_matching_index_is_disabled_errored_or_has_lot_of_errors()
        {
            Initialize();
            var definition = new AutoMapIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Name",
                    Storage = FieldStorage.No
                },
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE Name = 'Arek'"));

            var index = get_index(definition.Name);

            index.SetState(IndexState.Disabled);

            var result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);

            index.SetState(IndexState.Error);

            result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);

            index.SetPriority(IndexPriority.Normal);
            index._indexStorage.UpdateStats(DateTime.UtcNow, new IndexingRunStats()
            {
                MapAttempts = 1000,
                MapErrors = 900
            });

            result = _sut.Match(dynamicQuery, null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void Partial_match_if_analyzer_is_required()
        {
            Initialize();
            using (var db = CreateDocumentDatabase())
            {
                var mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"from Users
where Name = 'arek'"));

                db.IndexStore.CreateIndex(mapping.CreateAutoIndexDefinition()).Wait();

                mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"from Users
where search(Name, 'arek')"));

                var matcher = new DynamicQueryToIndexMatcher(db.IndexStore);

                var result = matcher.Match(mapping, null);

                Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            }
        }

        [Fact]
        public void Partial_match_if_exact_is_required()
        {
            Initialize();
            using (var db = CreateDocumentDatabase())
            {
                var mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"from Users
where Name = 'arek'"));

                db.IndexStore.CreateIndex(mapping.CreateAutoIndexDefinition()).Wait();

                mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"from Users
where exact(Name = 'arek')"));

                var matcher = new DynamicQueryToIndexMatcher(db.IndexStore);

                var result = matcher.Match(mapping, null);

                Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);
            }
        }

        [Fact]
        public void Partial_match_when_highlighting_is_required()
        {
            Initialize();
            using (var db = CreateDocumentDatabase())
            {
                var mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"from Users
where search(Name, 'arek')"));

                var definition = mapping.CreateAutoIndexDefinition();
                db.IndexStore.CreateIndex(definition).Wait();

                mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"from Users
where search(Name, 'arek')
include highlight(Name, 18, 2)
"));

                var matcher = new DynamicQueryToIndexMatcher(db.IndexStore);

                var result = matcher.Match(mapping, null);

                Assert.Equal(DynamicQueryMatchType.Partial, result.MatchType);

                mapping.ExtendMappingBasedOn(definition);

                definition = mapping.CreateAutoIndexDefinition();
                db.IndexStore.CreateIndex(definition).Wait();

                result = matcher.Match(mapping, null);

                Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            }
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
