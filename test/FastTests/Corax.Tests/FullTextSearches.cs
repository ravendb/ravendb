// -----------------------------------------------------------------------
//  <copyright file="BasicIndexUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Server.Indexing.Corax;
using Raven.Server.Indexing.Corax.Queries;
using Sparrow.Json.Parsing;
using Voron;
using Xunit;

namespace Tryouts.Corax.Tests
{
    public class FullTextSearch : CoraxTest
    {


        [Fact]
        public void CanIndexAndQueryWithBoolean()
        {
            using (var indexer = _fullTextIndex.CreateIndexer())
            {
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "John Doe",
                }, "users/2");
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "Michael Smith",
                }, "users/3");
            }

            using (var searcher = _fullTextIndex.CreateSearcher())
            {
                var ids = searcher.Query(new QueryDefinition
                {
                    Query = new BooleanQuery(QueryOperator.Or,
                        new TermQuery("Name", "john"),
                        new TermQuery("Name", "smith")
                        ),
                    Take = 2
                });
                Assert.Equal(new[] { "users/2", "users/3" }, ids);
            }
        }

        [Fact]
        public void CanIndexAndQuery()
        {
            using (var indexer = _fullTextIndex.CreateIndexer())
            {
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "John Doe",
                }, "users/2");
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "Michael Smith",
                }, "users/3");
            }

            using (var searcher = _fullTextIndex.CreateSearcher())
            {
                var ids = searcher.Query(new QueryDefinition
                {
                    Query = new TermQuery("Name", "smith"),
                    Take = 2
                });
                Assert.Equal(new[] { "users/3" }, ids);
            }
        }

        [Fact]
        public void WillScore()
        {
            using (var indexer = _fullTextIndex.CreateIndexer())
            {
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "John Doe",
                }, "users/2");
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "Michael Smith",
                }, "users/3");
            }

            using (var searcher = _fullTextIndex.CreateSearcher())
            {
                var ids = searcher.Query(new QueryDefinition
                {
                    Query = new BooleanQuery(QueryOperator.Or,
                    new TermQuery("Name", "smith"),
                    new TermQuery("Name", "michael"),
                    new TermQuery("Name", "doe")
                    ),
                    Take = 2
                });
                Assert.Equal(new[] { "users/3", "users/2" }, ids);
            }
        }

        [Fact]
        public void CanSort()
        {
            using (var indexer = _fullTextIndex.CreateIndexer())
            {
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "John Doe",
                    ["Symbol"] = "ABC"
                }, "users/2");
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "Michael Smith",
                    ["Symbol"] = "DEF"
                }, "users/3");
            }

            using (var searcher = _fullTextIndex.CreateSearcher())
            {
                var ids = searcher.Query(new QueryDefinition
                {
                    Query = new BooleanQuery(QueryOperator.Or,
                        new TermQuery("Name", "smith"),
                        new TermQuery("Name", "doe")
                        ),
                    Take = 2,
                    Sort = new[]
                    {
                        new QueryDefinition.OrderBy
                        {
                            Name = "Symbol",
                            Descending = true
                        },
                    }
                });
                Assert.Equal(new[] { "users/3", "users/2" }, ids);
            }
        }
    }

}