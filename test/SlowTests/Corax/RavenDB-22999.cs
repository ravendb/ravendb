using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax
{
    public class RavenDB_22999(ITestOutputHelper output) : RavenTestBase(output)
    {
        private Book _book1 = new Book() { Id = "books/1", Title = "Book1", };
        private Book _book2 = new Book() { Id = "books/2", Title = "Book2 ČĐŽŠĆ", };

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CaseInsensitiveStringComparisonTest(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(_book1, string.Empty, _book1.Id);
                await session.StoreAsync(_book2, string.Empty, _book2.Id);

                await session.SaveChangesAsync();
            }

            // 1) GOOD: ASCII characters comparison not matching the casing
            using (var session = store.OpenAsyncSession())
            {
                var books = await session.Query<Book>()
                    .Customize(c => c.WaitForNonStaleResults())
                    .Statistics(out var stats)
                    .Where(book => book.Title == "book1")
                    .ToListAsync();

                Assert.Single(books);
                Assert.Equal("Book1", books.First().Title);
                var terms = store.Maintenance.Send(new GetTermsOperation(stats.IndexName, nameof(Book.Title), null));
                Assert.Contains(_book1.Title.ToLowerInvariant(), terms);
                Assert.Contains(_book2.Title.ToLowerInvariant(), terms);
            }

            // 2) GOOD: UTF8 characters comparison matching the casing
            using (var session = store.OpenAsyncSession())
            {
                var books = await session.Query<Book>()
                    .Where(book => book.Title == "Book2 ČĐŽŠĆ")
                    .ToListAsync();

                Assert.Single(books);
                Assert.Equal("Book2 ČĐŽŠĆ", books.First().Title);
            }

            // 3) GOOD: UTF8 characters comparison ONLY ASCII CHARACTER DIFFERENT CASING
            using (var session = store.OpenAsyncSession())
            {
                var books = await session.Query<Book>()
                    .Where(book => book.Title == "BOOK2 ČĐŽŠĆ")
                    .ToListAsync();

                Assert.Single(books);
                Assert.Equal("Book2 ČĐŽŠĆ", books.First().Title);
            }

            // 4) FAILING: UTF8 characters comparison NON-ASCII CHARACTER DIFFERENT CASING
            using (var session = store.OpenAsyncSession())
            {
                var books = await session.Query<Book>()
                    .Where(book => book.Title == "Book2 čĐŽŠĆ")
                    .ToListAsync();

                Assert.Single(books);
                Assert.Equal("Book2 ČĐŽŠĆ", books.First().Title);
            }
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        public void BackwardCompatibilityForOldLowercaseAnalyzer()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var file = Path.Combine(backupPath, "RavenDB_22999.ravendb-snapshot");
            ExtractFile(file);
            using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
            var db = GetDatabaseName();
            using var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration {BackupLocation = backupPath, DatabaseName = db});
            
            using var session = store.OpenSession(database: db);
            
            var first = session
                .Query<Book>()
                .Statistics(out var stats)
                .Customize(x => x.WaitForNonStaleResults())
                .First(x => x.Title == _book1.Title);
            Assert.NotNull(first);
            
            var second = session
                .Query<Book>()
                .Customize(x => x.WaitForNonStaleResults())
                .First(x => x.Title == _book1.Title);
            Assert.NotNull(second);
            
            session.Store(new Book(){Title = "Ł_ASCI_Ł"});
            session.SaveChanges();
            Indexes.WaitForIndexing(store, databaseName: db);
            var third = session
                .Query<Book>()
                .Customize(x => x.WaitForNonStaleResults())
                .First(x => x.Title == "Ł_ASCI_Ł");
            Assert.NotNull(third);
            
            var terms = store.Maintenance.ForDatabase(databaseName: db).Send(new GetTermsOperation(stats.IndexName, nameof(Book.Title), null));
            Assert.Contains("book1", terms);
            Assert.Contains("book2 ČĐŽŠĆ", terms);
            Assert.Contains("Ł_asci_Ł", terms);
            
            void ExtractFile(string path)
            {
                using (var fileStream = File.Create(path))
                using (var stream = typeof(RavenDB_22999).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_22999.RavenDB-22999.ravendb-snapshot"))
                {
                    stream.CopyTo(fileStream);
                }
            }
        }

        public class Book
        {
            public string Id { get; set; }
            public string Title { get; set; }
        }
    }
}
