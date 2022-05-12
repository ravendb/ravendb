using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using MongoDB.Driver;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Migration
{
    public class MongoDBMigrationTest : RavenTestBase
    {
        public MongoDBMigrationTest(ITestOutputHelper output) : base(output)
        {
        }

        private class Book
        {
            public class Comparer : IEqualityComparer<Book>
            {
                public bool Equals(Book x, Book y) => x.Title == y.Title;
                public int GetHashCode(Book obj) => throw new NotImplementedException();
            }

            public string Title { get; set; }
        }

        private class Movie
        {
            public string Title { get; set; }

            public class Comparer : IEqualityComparer<Movie>
            {
                public bool Equals(Movie x, Movie y) => x.Title == y.Title;
                public int GetHashCode(Movie obj) => throw new NotImplementedException();
            }
        }

        [RequiresMongoDBFact]
        public async Task MigrateMongodb_WhenHasTwoCollectionAndImportAll_ShouldImportTheTwoCollectionWithAllThereDocuments()
        {
            var expectedBooks = new[] { new Book { Title = "Great Book1!!!" } };
            var expectedMovies = new[] { new Movie { Title = "Great Movie1!!!" } };

            var databaseName = $"Test{Guid.NewGuid().ToString()}";

            var connectionString = MongoDBConnectionString.Instance.ConnectionString.Value;
            var client = new MongoClient(connectionString);
            var db = client.GetDatabase(databaseName);

            try
            {
            var booksCollection = db.GetCollection<Book>("Books");
            await booksCollection.InsertManyAsync(expectedBooks);

            var moviesCollection = db.GetCollection<Movie>("Movies");
            await moviesCollection.InsertManyAsync(expectedMovies);


            var path = NewDataPath();
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
            Directory.CreateDirectory(path);

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var inputConfiguration = new DynamicJsonValue
                {
                    ["DatabaseName"] = databaseName,
                    ["Command"] = "export",
                    ["ExportFilePath"] = path,
                    ["ConnectionString"] = connectionString
                };
                var inputConfigurationStr = context.ReadObject(inputConfiguration, "InputConfiguration").ToString();

                Raven.Migrator.Program.Main(new[] { "mongodb", "-j", inputConfigurationStr });
            }

            using (var store = GetDocumentStore())
            {
                var databaseDumpFilePath = Directory.GetFiles(path).FirstOrDefault();
                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), databaseDumpFilePath);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                using (var session = store.OpenAsyncSession())
                {
                    var actualBooks = await session.Query<Book>().ToListAsync();
                    var actualMovies = await session.Query<Movie>().ToListAsync();

                    AssertEquivalent(expectedBooks, actualBooks, new Book.Comparer());
                    AssertEquivalent(expectedMovies, actualMovies, new Movie.Comparer());
                }
            }
        }
            finally
            {
                client.DropDatabase(databaseName);
            }
        }

        private static void AssertEquivalent<T>(IEnumerable<T> expected, IEnumerable<T> actual, IEqualityComparer<T> comparer)
        {
            Assert.Equal(expected.Count(), actual.Count());

            void Action(T b1) => Assert.Contains(b1, expected, comparer);

            var actions = expected.Select(b => (Action<T>)Action).ToArray();

            Assert.Collection(actual, actions);
        }
    }
}
