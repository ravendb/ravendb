// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1302.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Threading;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Common.Util;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1302 : RavenTest
    {
        private class Product
        {
        }

        [Fact]
        public void FirstOrDefaultShouldSetPageSizeToOne()
        {
            using (var store = NewRemoteDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;
                using (var session = store.OpenSession())
                {
                    id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;

                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .FirstOrDefault();

                    Assert.Null(product);
                }

                var profilingInformation = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profilingInformation.Requests.Count);
                Assert.Contains("pageSize=1", profilingInformation.Requests[0].Url);
            }
        }

        [Fact]
        public void FirstShouldSetPageSizeToOne()
        {
            using (new TemporaryCulture(CultureInfo.InvariantCulture))
            using (var store = NewRemoteDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;
                using (var session = store.OpenSession())
                {
                    id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;

                    var e = Assert.Throws<InvalidOperationException>(
                        () => session
                            .Advanced
                            .DocumentQuery<Product>()
                            .First());

                    Assert.Equal("Sequence contains no elements", e.Message);
                }

                var profilingInformation = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profilingInformation.Requests.Count);
                Assert.Contains("pageSize=1", profilingInformation.Requests[0].Url);
            }
        }

        [Fact]
        public void SingleOrDefaultShouldSetPageSizeToTwoIfItHasNotBeenSet()
        {
            using (var store = NewRemoteDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;

                using (var session = store.OpenSession())
                {
                    id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;

                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .SingleOrDefault();

                    Assert.Null(product);
                }

                var profilingInformation = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profilingInformation.Requests.Count);
                Assert.Contains("pageSize=2", profilingInformation.Requests[0].Url);
            }
        }

        [Fact]
        public void SingleShouldSetPageSizeToTwoIfItHasNotBeenSet()
        {
            using (new TemporaryCulture(CultureInfo.InvariantCulture))
            using (var store = NewRemoteDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;

                using (var session = store.OpenSession())
                {
                    id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;

                    var e = Assert.Throws<InvalidOperationException>(
                        () => session
                            .Advanced
                            .DocumentQuery<Product>()
                            .Single());

                    Assert.Equal("Sequence contains no elements", e.Message);
                }

                var profilingInformation = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profilingInformation.Requests.Count);
                Assert.Contains("pageSize=2", profilingInformation.Requests[0].Url);
            }
        }

        [Fact]
        public void FirstOrDefaultShouldSetPageSizeToOneIfItIsBigger()
        {
            using (var store = NewRemoteDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;
                using (var session = store.OpenSession())
                {
                    id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;

                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .Take(100)
                        .FirstOrDefault();

                    Assert.Null(product);
                }

                var profilingInformation = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profilingInformation.Requests.Count);
                Assert.Contains("pageSize=1", profilingInformation.Requests[0].Url);
            }
        }

        [Fact]
        public void FirstShouldSetPageSizeToOneIfItIsBigger()
        {
            using (new TemporaryCulture(CultureInfo.InvariantCulture))
            using (var store = NewRemoteDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;
                using (var session = store.OpenSession())
                {
                    id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;

                    var e = Assert.Throws<InvalidOperationException>(
                        () => session
                            .Advanced
                            .DocumentQuery<Product>()
                            .Take(100)
                            .First());

                    Assert.Equal("Sequence contains no elements", e.Message);
                }

                var profilingInformation = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profilingInformation.Requests.Count);
                Assert.Contains("pageSize=1", profilingInformation.Requests[0].Url);
            }
        }

        [Fact]
        public void SingleOrDefaultShouldSetPageSizeToTwoIfItIsBigger()
        {
            using (var store = NewRemoteDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;

                using (var session = store.OpenSession())
                {
                    id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;

                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .Take(100)
                        .SingleOrDefault();

                    Assert.Null(product);
                }

                var profilingInformation = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profilingInformation.Requests.Count);
                Assert.Contains("pageSize=2", profilingInformation.Requests[0].Url);
            }
        }

        [Fact]
        public void SingleShouldSetPageSizeToTwoIfItIsBigger()
        {
            using (new TemporaryCulture(CultureInfo.InvariantCulture))
            using (var store = NewRemoteDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;

                using (var session = store.OpenSession())
                {
                    id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;

                    var e = Assert.Throws<InvalidOperationException>(
                        () => session
                            .Advanced
                            .DocumentQuery<Product>()
                            .Take(100)
                            .Single());

                    Assert.Equal("Sequence contains no elements", e.Message);
                }

                var profilingInformation = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profilingInformation.Requests.Count);
                Assert.Contains("pageSize=2", profilingInformation.Requests[0].Url);
            }
        }

        [Fact]
        public void SingleOrDefaultShouldNotSetPageToTwoIfPageIsSmaller()
        {
            using (var store = NewRemoteDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;

                using (var session = store.OpenSession())
                {
                    id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;

                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .Take(1)
                        .SingleOrDefault();

                    Assert.Null(product);
                }

                var profilingInformation = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profilingInformation.Requests.Count);
                Assert.Contains("pageSize=1", profilingInformation.Requests[0].Url);
            }
        }

        [Fact]
        public void SingleShouldNotSetPageToTwoIfPageIsSmaller()
        {
            using (new TemporaryCulture(CultureInfo.InvariantCulture))
            using (var store = NewRemoteDocumentStore())
            {
                store.InitializeProfiling();

                Guid id;

                using (var session = store.OpenSession())
                {
                    id = ((DocumentSession)session).DatabaseCommands.ProfilingInformation.Id;

                    var e = Assert.Throws<InvalidOperationException>(
                        () => session
                            .Advanced
                            .DocumentQuery<Product>()
                            .Take(1)
                            .Single());

                    Assert.Equal("Sequence contains no elements", e.Message);
                }

                var profilingInformation = store.GetProfilingInformationFor(id);
                Assert.Equal(1, profilingInformation.Requests.Count);
                Assert.Contains("pageSize=1", profilingInformation.Requests[0].Url);
            }
        }

        [Fact]
        public void FirstOrDefaultShouldNotThrowIfSequenceIsEmpty()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .FirstOrDefault();

                    Assert.Null(product);
                }
            }
        }

        [Fact]
        public void FirstShouldThrowIfSequenceIsEmpty()
        {
            using (new TemporaryCulture(CultureInfo.InvariantCulture))
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidOperationException>(
                        () => session
                            .Advanced
                            .DocumentQuery<Product>()
                            .First());

                    Assert.Equal("Sequence contains no elements", e.Message);
                }
            }
        }

        [Fact]
        public void SingleOrDefaultShouldNotThrowIfSequenceIsEmpty()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .SingleOrDefault();

                    Assert.Null(product);
                }
            }
        }

        [Fact]
        public void SingleShouldThrowIfSequenceIsEmpty()
        {
            using (new TemporaryCulture(CultureInfo.InvariantCulture))
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidOperationException>(
                        () => session
                            .Advanced
                            .DocumentQuery<Product>()
                            .Single());

                    Assert.Equal("Sequence contains no elements", e.Message);
                }
            }
        }

        [Fact]
        public void FirstOrDefaultShouldNotThrowIfSequenceContainsOneElement()
        {
            using (var store = NewDocumentStore())
            {
                Fill(store, 1);

                using (var session = store.OpenSession())
                {
                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .WaitForNonStaleResultsAsOfNow()
                        .FirstOrDefault();

                    Assert.NotNull(product);
                }
            }
        }

        [Fact]
        public void FirstShouldNonThrowIfSequenceContainsOneElement()
        {
            using (var store = NewDocumentStore())
            {
                Fill(store, 1);

                using (var session = store.OpenSession())
                {
                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .WaitForNonStaleResultsAsOfNow()
                        .First();

                    Assert.NotNull(product);
                }
            }
        }

        [Fact]
        public void SingleOrDefaultShouldNotThrowIfSequenceContainsOneElement()
        {
            using (var store = NewDocumentStore())
            {
                Fill(store, 1);

                using (var session = store.OpenSession())
                {
                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .WaitForNonStaleResultsAsOfNow()
                        .SingleOrDefault();

                    Assert.NotNull(product);
                }
            }
        }

        [Fact]
        public void SingleShouldNotThrowIfSequenceContainsOneElement()
        {
            using (var store = NewDocumentStore())
            {
                Fill(store, 1);

                using (var session = store.OpenSession())
                {
                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .WaitForNonStaleResultsAsOfNow()
                        .Single();

                    Assert.NotNull(product);
                }
            }
        }

        [Fact]
        public void FirstOrDefaultShouldNotThrowIfSequenceContainsTwoElements()
        {
            using (var store = NewDocumentStore())
            {
                Fill(store, 2);

                using (var session = store.OpenSession())
                {
                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .WaitForNonStaleResultsAsOfNow()
                        .FirstOrDefault();

                    Assert.NotNull(product);
                }
            }
        }

        [Fact]
        public void FirstShouldNonThrowIfSequenceContainsTwoElements()
        {
            using (var store = NewDocumentStore())
            {
                Fill(store, 2);

                using (var session = store.OpenSession())
                {
                    var product = session
                        .Advanced
                        .DocumentQuery<Product>()
                        .WaitForNonStaleResultsAsOfNow()
                        .First();

                    Assert.NotNull(product);
                }
            }
        }

        [Fact]
        public void SingleOrDefaultShouldThrowIfSequenceContainsTwoElements()
        {
            using (new TemporaryCulture(CultureInfo.InvariantCulture))
            using (var store = NewDocumentStore())
            {
                Fill(store, 2);

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidOperationException>(
                        () => session
                            .Advanced
                            .DocumentQuery<Product>()
                            .WaitForNonStaleResultsAsOfNow()
                            .SingleOrDefault());

                    Assert.Equal("Sequence contains more than one element", e.Message);
                }
            }
        }

        [Fact]
        public void SingleShouldThrowIfSequenceContainsTwoElements()
        {
            using (new TemporaryCulture(CultureInfo.InvariantCulture))
            using (var store = NewDocumentStore())
            {
                Fill(store, 2);

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidOperationException>(
                         () => session
                             .Advanced
                             .DocumentQuery<Product>()
                             .WaitForNonStaleResultsAsOfNow()
                             .Single());

                    Assert.Equal("Sequence contains more than one element", e.Message);
                }
            }
        }

        private void Fill(IDocumentStore store, int count)
        {
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < count; i++)
                {
                    session.Store(new Product());
                }

                session.SaveChanges();
            }
        }
    }
}