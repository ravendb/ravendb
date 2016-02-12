using System;
using Raven.Abstractions.Data;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace FastTests.Server.Queries.Dynamic
{
    public class MatchingAutoIndexesForDynamicQueries : IDisposable
    {
        private readonly IndexStore _indexStore;
        private readonly DynamicQueryToIndexMatcher _sut;
        private readonly DocumentsStorage _documentsStorage;
        private readonly DatabaseNotifications _databaseNotifications;

        public MatchingAutoIndexesForDynamicQueries()
        {
            var config = new RavenConfiguration { Core = { RunInMemory = true } };

            _databaseNotifications = new DatabaseNotifications();
            _documentsStorage = new DocumentsStorage("TestStorage", config, _databaseNotifications);
            _documentsStorage.Initialize();

            _indexStore = new IndexStore(_documentsStorage, config.Indexing, _databaseNotifications);
            _indexStore.Initialize();

            _sut = new DynamicQueryToIndexMatcher(_indexStore);
        }

        [Fact]
        public void DoesNotMatchIfIndexStoreHasNoIndexes()
        {
            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "Name:Arek" });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        [Fact]
        public void IfThereIsOneMatchingIndexItShouldBeChosen()
        {
            var definition = new AutoIndexDefinition("Users", new[]
            {
                new AutoIndexField("Name"),
            });

            add_index(definition);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "Name:Arek" });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(definition.Name, result.IndexName);
        }

        [Fact]
        public void IndexMatchesAllFieldsShouldBeChosen()
        {
            var usersByName = new AutoIndexDefinition("Users", new[]
            {
                new AutoIndexField("Name"),
            });

            var usersByNameAndAge = new AutoIndexDefinition("Users", new[]
            {
                new AutoIndexField("Name"),
                new AutoIndexField("Age")
            });

            add_index(usersByName);
            add_index(usersByNameAndAge);

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQuery { Query = "Name:Arek Age:29" });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            Assert.Equal(usersByNameAndAge.Name, result.IndexName);
        }

        private void add_index(AutoIndexDefinition definition)
        {
            _indexStore.CreateIndex(definition);
        }

        public void Dispose()
        {
            _indexStore.Dispose();
            _documentsStorage.Dispose();
        }
    }
}