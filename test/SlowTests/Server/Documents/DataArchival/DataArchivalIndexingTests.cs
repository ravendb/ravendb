using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Json;
using Raven.Client.Util;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.DataArchival;

public class DataArchivalIndexingTests : RavenTestBase
{
    public DataArchivalIndexingTests(ITestOutputHelper output) : base(output)
    {
    }

    private async Task SetupDataArchival(IDocumentStore store)
    {
        var config = new DataArchivalConfiguration { Disabled = false, ArchiveFrequencyInSec = 100 };

        await DataArchivalHelper.SetupDataArchival(store, Server.ServerStore, config);
    }

    [Fact]
    public async Task CanIndexOnlyUnarchivedDocuments_AutoMapIndex()
    {
        using (var store = GetDocumentStore())
        {
            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store);

            // Make sure that the company is not skipped while indexing yet
            using (var session = store.OpenAsyncSession())
            {
                List<Company> companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(1, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                // Make sure that the company is skipped while indexing (auto map index)
                List<Company> companies = await session.Query<Company>().Where(x => x.Name == "Company Name").ToListAsync();
                Assert.Equal(0, companies.Count);
            }
        }
    }

    private class Companies_NamesCount : AbstractIndexCreationTask<Company, Companies_NamesCount.Result>
    {
        public class Result
        {
            public string Name { get; set; }
            public int Count { get; set; }
        }

        public Companies_NamesCount()
        {
            Map = companies => from company in companies
                               select new Result
                               {
                                   Name = company.Name,
                                   Count = 1
                               };

            Reduce = results => from result in results
                                group result by result.Name into g
                                select new Result
                                {
                                    Name = g.Key,
                                    Count = g.Sum(x => x.Count)
                                };
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CanIndexOnlyUnarchivedDocuments_MapReduceIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {

            // Spin up the index
            await new Companies_NamesCount().ExecuteAsync(store);

            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is not skipped while indexing yet
            using (var session = store.OpenAsyncSession())
            {
                await Indexes.WaitForIndexingAsync(store);
                var companies = await session.Query<Companies_NamesCount.Result, Companies_NamesCount>().ToListAsync();
                Assert.Equal(1, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                var companies = await session.Query<Companies_NamesCount.Result, Companies_NamesCount>().ToListAsync();
                Assert.Equal(0, companies.Count);
            }
        }
    }
    private class ArchivedCompanies_NamesCount : AbstractIndexCreationTask<Company, ArchivedCompanies_NamesCount.Result>
    {
        public class Result
        {
            public string Name { get; set; }
            public int Count { get; set; }
        }

        public ArchivedCompanies_NamesCount()
        {
            Map = companies => from company in companies
                               select new Result
                               {
                                   Name = company.Name,
                                   Count = 1
                               };

            Reduce = results => from result in results
                                group result by result.Name into g
                                select new Result
                                {
                                    Name = g.Key,
                                    Count = g.Sum(x => x.Count)
                                };

            ArchivedDataProcessingBehavior = Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior.ArchivedOnly;
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CanIndexOnlyArchivedDocuments_MapReduceIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {

            // Spin up the index
            await new ArchivedCompanies_NamesCount().ExecuteAsync(store);

            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is not skipped while indexing yet
            using (var session = store.OpenAsyncSession())
            {
                await Indexes.WaitForIndexingAsync(store);
                var companies = await session.Query<ArchivedCompanies_NamesCount.Result, ArchivedCompanies_NamesCount>().ToListAsync();
                Assert.Equal(0, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                await Indexes.WaitForIndexingAsync(store);
                // Make sure that the company is skipped while indexing (auto map index)
                var companies = await session.Query<ArchivedCompanies_NamesCount.Result, ArchivedCompanies_NamesCount>().ToListAsync();
                Assert.Equal(1, companies.Count);
            }
        }
    }

    private class Companies_AddressText : AbstractIndexCreationTask<Company, Companies_AddressText.IndexEntry>
    {
        public class IndexEntry
        {
            public string AddressText { get; set; }
        }

        public Companies_AddressText()
        {
            Map = companies => from company in companies
                               select new IndexEntry
                               {
                                   AddressText = company.Address1
                               };

            Index(x => x.AddressText, FieldIndexing.Search);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CanIndexOnlyUnarchivedDocuments_MapSearchIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            // Spin up the index
            await new Companies_AddressText().ExecuteAsync(store);


            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is not skipped while indexing yet
            using (var session = store.OpenAsyncSession())
            {

                await Indexes.WaitForIndexingAsync(store);
                List<Company> companies = await session.Query<Companies_AddressText.IndexEntry, Companies_AddressText>()
                    .Search(x => x.AddressText, "Dabrowskiego").OfType<Company>()
                    .ToListAsync();
                Assert.Equal(1, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                await Indexes.WaitForIndexingAsync(store);
                // Make sure that the company is skipped while indexing
                List<Company> companies = await session.Query<Companies_AddressText.IndexEntry, Companies_AddressText>()
                    .Search(x => x.AddressText, "Dabrowskiego").OfType<Company>()
                    .ToListAsync();
                Assert.Equal(0, companies.Count);
            }
        }
    }

    [Fact]
    public async Task CanIndexOnlyArchivedDocuments_AfterChangingConfiguration_MapSearchIndex()
    {
        Options options = new()
        {
            ModifyDatabaseRecord = dr =>
            {
                dr.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexArchivedDataProcessingBehavior)] = "ArchivedOnly";
            }
        };

        using (var store = GetDocumentStore(options))
        {
            // Spin up the index
            await new Companies_AddressText().ExecuteAsync(store);


            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is not skipped while indexing yet
            using (var session = store.OpenAsyncSession())
            {

                await Indexes.WaitForIndexingAsync(store);
                List<Company> companies = await session.Query<Companies_AddressText.IndexEntry, Companies_AddressText>()
                    .Search(x => x.AddressText, "Dabrowskiego").OfType<Company>()
                    .ToListAsync();
                Assert.Equal(0, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                await Indexes.WaitForIndexingAsync(store);
                // Make sure that the company is skipped while indexing
                List<Company> companies = await session.Query<Companies_AddressText.IndexEntry, Companies_AddressText>()
                    .Search(x => x.AddressText, "Dabrowskiego").OfType<Company>()
                    .ToListAsync();
                Assert.Equal(1, companies.Count);
            }
        }
    }

    private class ArchivedCompanies_AddressText : AbstractIndexCreationTask<Company, ArchivedCompanies_AddressText.IndexEntry>
    {
        public class IndexEntry
        {
            public string AddressText { get; set; }
        }

        public ArchivedCompanies_AddressText()
        {
            Map = companies => from company in companies
                               select new IndexEntry
                               {
                                   AddressText = company.Address1
                               };

            Index(x => x.AddressText, FieldIndexing.Search);
            ArchivedDataProcessingBehavior = Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior.ArchivedOnly;
        }
    }

    [Fact]
    public async Task CanGetArchivedDataProcessingBehaviorFromIndexStatistics()
    {
        using (var store = GetDocumentStore())
        {
            // Spin up the index
            await new ArchivedCompanies_AddressText().ExecuteAsync(store);

            var op = new GetIndexStatisticsOperation("ArchivedCompanies/AddressText");
            Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, store.Maintenance.Send(op).ArchivedDataProcessingBehavior);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CanIndexOnlyArchivedDocuments_MapSearchIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            // Spin up the index
            await new ArchivedCompanies_AddressText().ExecuteAsync(store);


            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is skipped from indexing while being unarchived yet
            using (var session = store.OpenAsyncSession())
            {

                await Indexes.WaitForIndexingAsync(store);
                List<Company> companies = await session.Query<ArchivedCompanies_AddressText.IndexEntry, ArchivedCompanies_AddressText>()
                    .Search(x => x.AddressText, "Dabrowskiego").OfType<Company>()
                    .ToListAsync();
                Assert.Equal(0, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                await Indexes.WaitForIndexingAsync(store);
                // Make sure that the company is not anymore skipped while indexing
                List<Company> companies = await session.Query<ArchivedCompanies_AddressText.IndexEntry, ArchivedCompanies_AddressText>()
                    .Search(x => x.AddressText, "Dabrowskiego").OfType<Company>()
                    .ToListAsync();
                Assert.Equal(1, companies.Count);
            }
        }
    }

    private class AllCompanies_AddressText : AbstractIndexCreationTask<Company, AllCompanies_AddressText.IndexEntry>
    {
        public class IndexEntry
        {
            public string AddressText { get; set; }
        }

        public AllCompanies_AddressText()
        {
            Map = companies => from company in companies
                               select new IndexEntry
                               {
                                   AddressText = company.Address1
                               };

            Index(x => x.AddressText, FieldIndexing.Search);
            ArchivedDataProcessingBehavior = Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior.IncludeArchived;
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CanIndexAllDocumentsAndArchivedDocuments_MapSearchIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            // Spin up the index
            await new AllCompanies_AddressText().ExecuteAsync(store);


            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var company2 = new Company { Name = "OG IT", Address1 = "Dabrowskiego 6/9" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                await session.StoreAsync(company2);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is not skipped while indexing yet
            using (var session = store.OpenAsyncSession())
            {

                await Indexes.WaitForIndexingAsync(store);
                List<Company> companies = await session.Query<AllCompanies_AddressText.IndexEntry, AllCompanies_AddressText>()
                    .Search(x => x.AddressText, "Dabrowskiego").OfType<Company>()
                    .ToListAsync();
                Assert.Equal(2, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                var unarchivedCompany = await session.LoadAsync<Company>(company2.Id);
                var metadata2 = session.Advanced.GetMetadataFor(unarchivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata2.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata2.Keys);
                Assert.DoesNotContain(Constants.Documents.Metadata.Archived, metadata2.Keys);


                await Indexes.WaitForIndexingAsync(store);
                // Make sure that no company is being skipped while indexing
                List<Company> companies = await session.Query<AllCompanies_AddressText.IndexEntry, AllCompanies_AddressText>()
                    .Search(x => x.AddressText, "Dabrowskiego").OfType<Company>()
                    .ToListAsync();
                Assert.Equal(2, companies.Count);
            }
        }
    }


    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CanFilterOutUnarchivedDocumentsFromIndex_MapIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            // Spin-up the index
            store.Maintenance.Send(new PutIndexesOperation(new[] {
                new IndexDefinition
                {
                    Maps = {"from o in docs where o[\"@metadata\"][\"@archived\"] == true select new" +
                        "{" +
                        "    Name = o.Name" +
                        "}"},
                    Name = "test",
                    ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.IncludeArchived
                }}));


            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is skipped from indexing by map
            using (var session = store.OpenAsyncSession())
            {
                await Indexes.WaitForIndexingAsync(store);
                var entries = await session.Query<User>("test").Customize(x => x.WaitForNonStaleResults()).ToListAsync();
                Assert.Equal(0, entries.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            // Make sure that the company is not skipped from indexing by map
            using (var session = store.OpenAsyncSession())
            {
                await Indexes.WaitForIndexingAsync(store);
                var entries = await session.Query<User>("test").Customize(x => x.WaitForNonStaleResults()).ToListAsync();
                Assert.Equal(1, entries.Count);
            }
        }
    }



    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CanFilterOutArchivedDocumentsFromIndex_MapIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            // Spin-up the index
            store.Maintenance.Send(new PutIndexesOperation(new[] {
                new IndexDefinition
                {
                    Maps = {"from o in docs where o[\"@metadata\"][\"@archived\"] == null select new" +
                        "{" +
                        "    Name = o.Name" +
                        "}"},
                    Name = "test",
                    ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.IncludeArchived
                }}));


            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is skipped from indexing by map
            using (var session = store.OpenAsyncSession())
            {
                await Indexes.WaitForIndexingAsync(store);
                var entries = await session.Query<User>("test").Customize(x => x.WaitForNonStaleResults()).ToListAsync();
                Assert.Equal(1, entries.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            // Make sure that the company is not skipped from indexing by map
            using (var session = store.OpenAsyncSession())
            {
                await Indexes.WaitForIndexingAsync(store);
                var entries = await session.Query<User>("test").Customize(x => x.WaitForNonStaleResults()).ToListAsync();
                Assert.Equal(0, entries.Count);
            }
        }
    }

    private class InvalidCountersIndexDefinitionWithItemKind : AbstractCountersIndexCreationTask<Employee>
    {
        private class Result
        {
            public string CompanyName { get; set; }
        }

        public InvalidCountersIndexDefinitionWithItemKind()
        {
            AddMap("Companies",
                counters => from counter in counters
                            select new Result
                            {
                                CompanyName = LoadDocument<Company>(counter.Name).Name
                            });
            ArchivedDataProcessingBehavior = Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior.ExcludeArchived;
        }
    }

    private class InvalidTimeSeriesDefinitionWithItemKind : AbstractTimeSeriesIndexCreationTask<Company>
    {
        private class Result
        {
            public string Name { get; set; }

            public int Count { get; set; }

            public double[] Min { get; set; }

            public double[] Max { get; set; }

            public double[] Sum { get; set; }

            public double LastMin { get; set; }

            public double FirstMax { get; set; }
        }

        public InvalidTimeSeriesDefinitionWithItemKind()
        {
            AddMap("TS", segments => from segment in segments
                                     select new Result
                                     {
                                         Name = segment.Name,
                                         Count = segment.Count,
                                         FirstMax = segment.Max[0],
                                         LastMin = segment.Min.Last(),
                                         Max = segment.Max,
                                         Min = segment.Min,
                                         Sum = segment.Sum
                                     });
            ArchivedDataProcessingBehavior = Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior.ExcludeArchived;
        }
    }


    [Fact]
    public async Task SettingArchivedDataProcessingBehaviorIndexWillThrowNotSupportedException()
    {
        using (var store = GetDocumentStore())
        {
            await Assert.ThrowsAsync<RavenException>(async () => await new InvalidCountersIndexDefinitionWithItemKind().ExecuteAsync(store));
        }
    }

    [Fact]
    public async Task SettingArchivedDataProcessingBehaviorOnTimeSeriesIndexWillThrowNotSupportedException()
    {
        using (var store = GetDocumentStore())
        {
            await Assert.ThrowsAsync<RavenException>(async () => await new InvalidTimeSeriesDefinitionWithItemKind().ExecuteAsync(store));
        }
    }



    private class AnyTimeSeriesIndex : AbstractTimeSeriesIndexCreationTask<Company>
    {
        public class Result
        {
            public string Name { get; set; }

            public int Count { get; set; }

            public double[] Min { get; set; }

            public double[] Max { get; set; }

            public double[] Sum { get; set; }

            public double LastMin { get; set; }

            public double FirstMax { get; set; }
        }

        public AnyTimeSeriesIndex()
        {
            AddMap("TS", segments => from segment in segments
                                     select new Result
                                     {
                                         Name = segment.Name,
                                         Count = segment.Count,
                                         FirstMax = segment.Max[0],
                                         LastMin = segment.Min.Last(),
                                         Max = segment.Max,
                                         Min = segment.Min,
                                         Sum = segment.Sum
                                     });
        }
    }

    private class AnyCountersIndex : AbstractCountersIndexCreationTask<Employee>
    {
        public class Result
        {
            public string CompanyName { get; set; }
        }

        public AnyCountersIndex()
        {
            AddMap("Companies",
                counters => from counter in counters
                            select new Result
                            {
                                CompanyName = LoadDocument<Company>(counter.Name).Name
                            });
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task DataArchivalWontAffectIndexingDocumentsCounters(Options options)
    {
        const string commonName = "Companies";
        const string companyName1 = "OG IT";
        const int employeesCount = 1;
        using (var store = GetDocumentStore(options))
        {
            // Spin up the index
            await new AnyCountersIndex().ExecuteAsync(store);

            var retires = SystemTime.UtcNow.AddMinutes(5);

            var company = new Company
            {
                Id = commonName,
                Name = companyName1
            };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                await session.SaveChangesAsync();
                await using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < employeesCount; i++)
                    {
                        var employee = new Employee();
                        await bulk.StoreAsync(employee,
                            new MetadataAsDictionary { new(Constants.Documents.Metadata.ArchiveAt, retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)) });
                        await bulk.CountersFor(employee.Id).IncrementAsync(commonName);
                    }
                }
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                await session.SaveChangesAsync();
                await using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < employeesCount; i++)
                    {
                        var employee = new Employee();
                        await bulk.StoreAsync(employee);
                        await bulk.CountersFor(employee.Id).IncrementAsync(commonName);
                    }
                }
            }

            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(3));

            // Make sure that no docs are skipped while indexing
            using (var session = store.OpenAsyncSession())
            {
                List<AnyCountersIndex.Result> entries = await session.Query<AnyCountersIndex.Result, AnyCountersIndex>().ToListAsync();
                Assert.Equal(2, entries.Count);

                var counters = await session.CountersFor("employees/1-A").GetAllAsync();
                Assert.Equal(1, counters.Count);
            }

            // Activate the archival manually
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();


            // Assert
            using (var session = store.OpenAsyncSession())
            {
                var archivedEmployee = await session.LoadAsync<Employee>("employees/1-A");
                var metadata = session.Advanced.GetMetadataFor(archivedEmployee);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);
                Assert.True(metadata[Constants.Documents.Metadata.Flags].ToString().Contains("Archived"));
                var counters = await session.CountersFor(archivedEmployee).GetAllAsync();
                Assert.Equal(1, counters.Count);

                await Indexes.WaitForIndexingAsync(store);
                // Make sure that no company is skipped while indexing
                List<AnyCountersIndex.Result> entries = await session.Query<AnyCountersIndex.Result, AnyCountersIndex>().ToListAsync();
                Assert.Equal(2, entries.Count);
            }
        }
    }


    [RavenTheory(RavenTestCategory.TimeSeries)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task DataArchivalWontAffectIndexingDocumentsTimeSeries(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            await new AnyTimeSeriesIndex().ExecuteAsync(store);
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var company2 = new Company { Name = "OG IT", Address1 = "Julianowo 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                session.TimeSeriesFor(company, "TS").Append(DateTime.Now, new[] { 3, 5.5 });
                session.TimeSeriesFor(company, "TS").Append(DateTime.Now.AddMilliseconds(10), new[] { 2, 3.5 });

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company2);
                session.TimeSeriesFor(company2, "TS").Append(DateTime.Now, new[] { 3, 5.5 });
                session.TimeSeriesFor(company2, "TS").Append(DateTime.Now.AddMilliseconds(10), new[] { 2, 3.5 });

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<AnyTimeSeriesIndex.Result, AnyTimeSeriesIndex>()
                    .ToListAsync();
                Assert.Equal(2, results.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);
            using (var session = store.OpenSession())
            {
                var results = session
                    .Query<AnyTimeSeriesIndex.Result, AnyTimeSeriesIndex>()
                    .ToList();

                Assert.Equal(2, results.Count);

                var result = results[0];

                Assert.Equal(new[] { 2, 3.5 }, result.Min);
                Assert.Equal(new double[] { 5, 9 }, result.Sum);
                Assert.Equal(new[] { 3, 5.5 }, result.Max);
                Assert.Equal(2, result.Count);
                Assert.Equal(3, result.FirstMax);
                Assert.Equal(3.5, result.LastMin);
            }
        }
    }

    private class CompaniesByNameJS : AbstractJavaScriptIndexCreationTask
    {
        public CompaniesByNameJS()
        {
            Maps = new HashSet<string>
            {
                @"map('Companies', function (u){ return { Name: u.Name, Count: 1};})",
            };
        }

    }

    private class ArchivedCompaniesByNameJS : AbstractJavaScriptIndexCreationTask
    {
        public ArchivedCompaniesByNameJS()
        {
            Maps = new HashSet<string>
            {
                @"map('Companies', function (u){ return { Name: u.Name, Count: 1};})",
            };
            ArchivedDataProcessingBehavior = Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior.ArchivedOnly;
        }
    }

    private class AllCompaniesByNameJS : AbstractJavaScriptIndexCreationTask
    {
        public AllCompaniesByNameJS()
        {
            Maps = new HashSet<string>
            {
                @"map('Companies', function (u){ return { Name: u.Name, Count: 1};})",
            };
            ArchivedDataProcessingBehavior = Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior.IncludeArchived;
        }
    }

    private class CompaniesByNameJSMapReduce : AbstractJavaScriptIndexCreationTask
    {
        public CompaniesByNameJSMapReduce()
        {
            Maps = new HashSet<string> {@"map('Companies', function (u){ return { Name: u.Name, Count: 1, Id: u.Id};})",};

            Reduce = @"groupBy(x => ({ Name: x.Name }))
.aggregate(g => { 
    return {
        Name: g.key.Name,
        Count: g.values.reduce((count, val) => val.Count + count, 0),
    };
})";
        }
    }

    private class ArchivedCompaniesByNameJSMapReduce : AbstractJavaScriptIndexCreationTask
    {
        public ArchivedCompaniesByNameJSMapReduce()
        {
            Maps = new HashSet<string>
            {
                @"map('Companies', function (u){ return { Name: u.Name, Count: 1, Id: u.Id};})",
            };
            
            Reduce = @"groupBy(x => ({ Name: x.Name }))
.aggregate(g => { 
    return {
        Name: g.key.Name,
        Count: g.values.reduce((count, val) => val.Count + count, 0),
    };
})";
            ArchivedDataProcessingBehavior = Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior.ArchivedOnly;
        }
    }

    private class AllCompaniesByNameJSMapReduce : AbstractJavaScriptIndexCreationTask
    {
        public AllCompaniesByNameJSMapReduce()
        {
            Maps = new HashSet<string>
            {
                @"map('Companies', function (u){ return { Name: u.Name, Count: 1, Id: u.Id};})",
            };

            Reduce = @"groupBy(x => ({ Name: x.Name }))
.aggregate(g => { 
    return {
        Name: g.key.Name,
        Count: g.values.reduce((count, val) => val.Count + count, 0),
    };
})";
            ArchivedDataProcessingBehavior = Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior.IncludeArchived;
        }
    }


    [Fact]
    public async Task CanIndexOnlyUnarchivedDocuments_JavaScriptMapReduceIndex()
    {
        using (var store = GetDocumentStore())
        {
            await new CompaniesByNameJSMapReduce().ExecuteAsync(store);
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var company2 = new Company { Name = "OG IT", Address1 = "Julianowo 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.StoreAsync(company2);
                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("CompaniesByNameJSMapReduce")
                    .ToListAsync();
                Assert.Equal(2, results.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("CompaniesByNameJSMapReduce")
                    .ToListAsync();
                Assert.Equal(1, results.Count);
            }
        }
    }


    [Fact]
    public async Task CanIndexOnlyArchivedDocuments_JavaScriptMapReduceIndex()
    {
        using (var store = GetDocumentStore())
        {
            await new ArchivedCompaniesByNameJSMapReduce().ExecuteAsync(store);
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var company2 = new Company { Name = "OG IT", Address1 = "Julianowo 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.StoreAsync(company2);
                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("ArchivedCompaniesByNameJSMapReduce")
                    .ToListAsync();
                Assert.Equal(0, results.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("ArchivedCompaniesByNameJSMapReduce")
                    .ToListAsync();
                Assert.Equal(1, results.Count);
            }
        }
    }

    [Fact]
    public async Task CanIndexAllDocuments_JavaScriptMapReduceIndex()
    {
        using (var store = GetDocumentStore())
        {
            await new AllCompaniesByNameJSMapReduce().ExecuteAsync(store);
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var company2 = new Company { Name = "OG IT", Address1 = "Julianowo 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.StoreAsync(company2);
                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("AllCompaniesByNameJSMapReduce")
                    .ToListAsync();
                Assert.Equal(2, results.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("AllCompaniesByNameJSMapReduce")
                    .ToListAsync();
                Assert.Equal(2, results.Count);
            }
        }
    }
    
    [Fact]
    public async Task CanIndexOnlyUnarchivedDocuments_JavaScriptMapIndex()
    {
        using (var store = GetDocumentStore())
        {
            await new CompaniesByNameJS().ExecuteAsync(store);
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var company2 = new Company { Name = "OG IT", Address1 = "Julianowo 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.StoreAsync(company2);
                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("CompaniesByNameJS")
                    .ToListAsync();
                Assert.Equal(2, results.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("CompaniesByNameJS")
                    .ToListAsync();
                Assert.Equal(1, results.Count);
            }
        }
    }


    [Fact]
    public async Task CanIndexOnlyArchivedDocuments_JavaScriptMapIndex()
    {
        using (var store = GetDocumentStore())
        {
            await new ArchivedCompaniesByNameJS().ExecuteAsync(store);
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var company2 = new Company { Name = "OG IT", Address1 = "Julianowo 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.StoreAsync(company2);
                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("ArchivedCompaniesByNameJS")
                    .ToListAsync();
                Assert.Equal(0, results.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("ArchivedCompaniesByNameJS")
                    .ToListAsync();
                Assert.Equal(1, results.Count);
            }
        }
    }

    [Fact]
    public async Task CanIndexAllDocuments_JavaScriptMapIndex()
    {
        using (var store = GetDocumentStore())
        {
            await new AllCompaniesByNameJS().ExecuteAsync(store);
            var company = new Company { Name = "Company Name", Address1 = "Dabrowskiego 6" };
            var company2 = new Company { Name = "OG IT", Address1 = "Julianowo 6" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.StoreAsync(company2);
                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store);
            RavenTestHelper.AssertNoIndexErrors(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("AllCompaniesByNameJS")
                    .ToListAsync();
                Assert.Equal(2, results.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<Company>("AllCompaniesByNameJS")
                    .ToListAsync();
                Assert.Equal(2, results.Count);
            }
        }
    }

    private class InvalidJSCountersIndex : AbstractJavaScriptCountersIndexCreationTask
    {
        public InvalidJSCountersIndex()
        {
            Maps = new HashSet<string>
            {
                @"counters.map('Companies', 'HeartRate', function (counter) {
return {
HeartBeat: counter.Value,
Name: counter.Name,
User: counter.DocumentId
};
})"
            };
            ArchivedDataProcessingBehavior = Raven.Client.Documents.DataArchival.ArchivedDataProcessingBehavior.ExcludeArchived;
        }
    }

    [Fact]
    public async Task SettingArchivedDataProcessingBehaviorOnJSCountersIndexWillThrowNotSupportedException()
    {
        using (var store = GetDocumentStore())
        {
            await Assert.ThrowsAsync<RavenException>(async () => await new InvalidJSCountersIndex().ExecuteAsync(store));
        }
    }

    [Fact]
    public async Task CanChangeDefaultArchivedDataProcessingBehaviorForAutoIndexes()
    {
        Options options = new()
        {
            ModifyDatabaseRecord = dr =>
            {
                dr.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexArchivedDataProcessingBehavior)] = "ArchivedOnly";
            }
        };
        using (var store = GetDocumentStore(options: options))
        {
            // Insert document with archive time before activating the archival
            var company = new Company { Name = "OG IT", Email = "gracjan@ravendb.net" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is skipped while indexing yet
            using (var session = store.OpenAsyncSession())
            {
                List<Company> companies = await session.Query<Company>().Where(x => x.Name == "OG IT").ToListAsync();
                WaitForUserToContinueTheTest(store);
                Assert.Equal(0, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                await Indexes.WaitForIndexingAsync(store);
                // Make sure that the company is not skipped while indexing (auto map index)
                List<Company> companies = await session.Query<Company>().Where(x => x.Email == "gracjan@ravendb.net").ToListAsync();
                Assert.Equal(1, companies.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CanIndexOnlyArchivedDocuments_IndexBuilder(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            // Spin up the index
            const string indexName = "BuilderMapIndex";
            var indexDefinition = new IndexDefinitionBuilder<Company, Company>
            {
                Map = companies => from company in companies where company.Name == "Company Name" select new { company.Name },
            }.ToIndexDefinition(store.Conventions);
            indexDefinition.Name = indexName;
            indexDefinition.ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly;
            store.Maintenance.Send(new PutIndexesOperation(indexDefinition));
            await Indexes.WaitForIndexingAsync(store);


            // Insert document with archive time before activating the archival
            var company = new Company { Name = "Company Name" };
            var retires = SystemTime.UtcNow.AddMinutes(5);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                var metadata = session.Advanced.GetMetadataFor(company);
                metadata[Constants.Documents.Metadata.ArchiveAt] = retires.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);
                await session.SaveChangesAsync();
            }

            // Make sure that the company is skipped while indexing
            using (var session = store.OpenAsyncSession())
            {
                await Indexes.WaitForIndexingAsync(store);
                var companies = await session.Query<Company>(indexName).ToListAsync();
                Assert.Equal(0, companies.Count);
            }

            // Activate the archival
            await SetupDataArchival(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
            var documentsArchiver = database.DataArchivist;
            await documentsArchiver.ArchiveDocs();

            using (var session = store.OpenAsyncSession())
            {
                var archivedCompany = await session.LoadAsync<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(archivedCompany);
                Assert.DoesNotContain(Constants.Documents.Metadata.ArchiveAt, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Collection, metadata.Keys);
                Assert.Contains(Constants.Documents.Metadata.Archived, metadata.Keys);
                Assert.Equal(true, metadata[Constants.Documents.Metadata.Archived]);

                await Indexes.WaitForIndexingAsync(store);
                // Make sure that the company is not skipped anymore while indexing
                var companies = await session.Query<Company>(indexName).ToListAsync();
                Assert.Equal(1, companies.Count);
            }
        }
    }
}
