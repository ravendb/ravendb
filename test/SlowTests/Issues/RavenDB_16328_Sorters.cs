using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Sorters;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Exceptions.Documents.Sorters;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations.Sorters;
using Raven.Server.Documents.Queries;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16328_Sorters : RavenTestBase
    {
        public RavenDB_16328_Sorters(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseCustomSorter()
        {
            var sorterName = GetDatabaseName();
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => sorterName
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "C1" });
                    session.Store(new Company { Name = "C2" });

                    session.SaveChanges();
                }

                CanUseSorterInternal<SorterDoesNotExistException>(store, $"There is no sorter with '{sorterName}' name", $"There is no sorter with '{sorterName}' name", sorterName);

                var sorterCode = GetSorter("RavenDB_8355.MySorter.cs", "MySorter", sorterName);

                store.Maintenance.Server.Send(new PutServerWideSortersOperation(new SorterDefinition
                {
                    Name = sorterName,
                    Code = sorterCode
                }));

                // checking if we can send again same sorter
                store.Maintenance.Server.Send(new PutServerWideSortersOperation(new SorterDefinition
                {
                    Name = sorterName,
                    Code = sorterCode
                }));

                CanUseSorterInternal<RavenException>(store, "Catch me: Name:2:0:False", "Catch me: Name:2:0:True", sorterName);

                sorterCode = sorterCode.Replace("Catch me", "Catch me 2");

                // checking if we can update sorter
                store.Maintenance.Server.Send(new PutServerWideSortersOperation(new SorterDefinition
                {
                    Name = sorterName,
                    Code = sorterCode
                }));

                var e = Assert.Throws<SorterCompilationException>(() =>
                {
                    // We should not be able to add sorter with non-matching name
                    store.Maintenance.Server.Send(new PutServerWideSortersOperation(new SorterDefinition
                    {
                        Name = $"{sorterName}_OtherName",
                        Code = sorterCode
                    }));
                });

                Assert.Contains($"Could not find type '{sorterName}_OtherName' in given assembly.", e.Message);

                CanUseSorterInternal<RavenException>(store, "Catch me 2: Name:2:0:False", "Catch me 2: Name:2:0:True", sorterName);

                store.Maintenance.Server.Send(new DeleteServerWideSorterOperation(sorterName));

                CanUseSorterInternal<SorterDoesNotExistException>(store, $"There is no sorter with '{sorterName}' name", $"There is no sorter with '{sorterName}' name", sorterName);
            }
        }

        [Fact]
        public void CanOverrideCustomSorter()
        {
            var sorterName = GetDatabaseName();
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => sorterName
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "C1" });
                    session.Store(new Company { Name = "C2" });

                    session.SaveChanges();
                }

                CanUseSorterInternal<SorterDoesNotExistException>(store, $"There is no sorter with '{sorterName}' name", $"There is no sorter with '{sorterName}' name", sorterName);

                var sorterCode = GetSorter("RavenDB_8355.MySorter.cs", "MySorter", sorterName);

                store.Maintenance.Server.Send(new PutServerWideSortersOperation(new SorterDefinition
                {
                    Name = sorterName,
                    Code = sorterCode
                }));

                CanUseSorterInternal<RavenException>(store, "Catch me: Name:2:0:False", "Catch me: Name:2:0:True", sorterName);

                sorterCode = sorterCode.Replace("Catch me", "Catch me 2");

                store.Maintenance.Send(new PutSortersOperation(new SorterDefinition
                {
                    Name = sorterName,
                    Code = sorterCode
                }));

                CanUseSorterInternal<RavenException>(store, "Catch me 2: Name:2:0:False", "Catch me 2: Name:2:0:True", sorterName);

                store.Maintenance.Send(new DeleteSorterOperation(sorterName));

                CanUseSorterInternal<RavenException>(store, "Catch me: Name:2:0:False", "Catch me: Name:2:0:True", sorterName);
            }
        }

        [Fact]
        public void CanGetCustomSorterDiagnostics()
        {
            var sorterName = GetDatabaseName();
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = _ => sorterName
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "C1" });
                    session.Store(new Company { Name = "C2" });

                    session.SaveChanges();
                }

                store.Maintenance.Server.Send(new PutServerWideSortersOperation(new SorterDefinition
                {
                    Name = $"{sorterName}_WithDiagnostics",
                    Code = GetSorter("RavenDB_8355.MySorterWithDiagnostics.cs", "MySorterWithDiagnostics", $"{sorterName}_WithDiagnostics")
                }));

                var diagnostics = store.Operations.Send(new CustomQueryOperation($"from Companies order by custom(Name, '{sorterName}_WithDiagnostics')"));

                Assert.True(diagnostics.Count > 0);
                Assert.Contains("Inner", diagnostics);
            }
        }

        private static void CanUseSorterInternal<TException>(DocumentStore store, string asc, string desc, string sorterName)
            where TException : RavenException
        {
            using (var session = store.OpenSession())
            {
                var e = Assert.Throws<TException>(() =>
                {
                    session
                        .Advanced
                        .RawQuery<Company>($"from Companies order by custom(Name, '{sorterName}')")
                        .ToList();
                });

                Assert.Contains(asc, e.Message);

                e = Assert.Throws<TException>(() =>
                {
                    session
                        .Query<Company>()
                        .OrderBy(x => x.Name, sorterName)
                        .ToList();
                });

                Assert.Contains(asc, e.Message);

                e = Assert.Throws<TException>(() =>
                {
                    session
                        .Advanced
                        .DocumentQuery<Company>()
                        .OrderBy(x => x.Name, sorterName)
                        .ToList();
                });

                Assert.Contains(asc, e.Message);

                e = Assert.Throws<TException>(() =>
                {
                    session
                        .Advanced
                        .RawQuery<Company>($"from Companies order by custom(Name, '{sorterName}') desc")
                        .ToList();
                });

                Assert.Contains(desc, e.Message);

                e = Assert.Throws<TException>(() =>
                {
                    session
                        .Query<Company>()
                        .OrderByDescending(x => x.Name, sorterName)
                        .ToList();
                });

                Assert.Contains(desc, e.Message);

                e = Assert.Throws<TException>(() =>
                {
                    session
                        .Advanced
                        .DocumentQuery<Company>()
                        .OrderByDescending(x => x.Name, sorterName)
                        .ToList();
                });

                Assert.Contains(desc, e.Message);
            }
        }

        private class CustomQueryOperation : IOperation<List<string>>
        {
            private readonly string _rql;

            public CustomQueryOperation(string rql)
            {
                _rql = rql;
            }

            public RavenCommand<List<string>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
            {
                return new CustomQueryCommand(_rql);
            }

            private class CustomQueryCommand : RavenCommand<List<string>>
            {
                private readonly string _rql;

                public CustomQueryCommand(string rql)
                {
                    _rql = rql;
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/queries?query={Uri.EscapeDataString(_rql)}&diagnostics=true";
                    return new HttpRequestMessage(HttpMethod.Get, url);
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    Result = new List<string>();

                    response.TryGet(nameof(IndexQueryServerSide.Diagnostics), out BlittableJsonReaderArray array);

                    foreach (var item in array)
                        Result.Add(item.ToString());
                }
            }
        }

        private static string GetSorter(string resourceName, string originalSorterName, string sorterName)
        {
            using (var stream = GetDump(resourceName))
            using (var reader = new StreamReader(stream))
            {
                var analyzerCode = reader.ReadToEnd();
                analyzerCode = analyzerCode.Replace(originalSorterName, sorterName);

                return analyzerCode;
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_8355).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
