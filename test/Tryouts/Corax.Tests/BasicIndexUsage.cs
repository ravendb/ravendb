// -----------------------------------------------------------------------
//  <copyright file="BasicIndexUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Server.Json.Parsing;
using Tryouts.Corax.Analyzers;
using Voron;
using Xunit;

namespace Tryouts.Corax.Tests
{
    public class BasicIndexUsage : CoraxTest
    {
        protected override IAnalyzer CreateAnalyzer()
        {
            return new NopAnalyzer();
        }

        [Fact]
        public void CanIndex()
        {
            using (var indexer = _fullTextIndex.CreateIndexer())
            {
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "Michae",
                }, "users/2");
                indexer.NewEntry(new DynamicJsonValue
                {
                    ["Name"] = "Michae",
                }, "users/2");
            }
        }
    }

    public class CoraxTest : IDisposable
    {
        protected readonly FullTextIndex _fullTextIndex;

        protected virtual IAnalyzer CreateAnalyzer()
        {
            return new DefaultAnalyzer();
        }

        public CoraxTest()
        {
            _fullTextIndex = new FullTextIndex(StorageEnvironmentOptions.CreateMemoryOnly(), CreateAnalyzer());
        }

        public void Dispose()
        {
            _fullTextIndex?.Dispose();
        }
    }
}