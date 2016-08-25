// -----------------------------------------------------------------------
//  <copyright file="Kijana.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Kijana : RavenTestBase
    {
        private class Scratch
        {
            public string Id { get; set; }
            public long Value { get; set; }
        }

        private class ScratchIndex : AbstractIndexCreationTask<Scratch>
        {
            public ScratchIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        doc.Value
                    };

                Sort(x => x.Value, SortOptions.NumericDefault);
            }
        }

        [Fact]
        public void CanSetSortValue()
        {
            using (var store = GetDocumentStore())
            {
                new ScratchIndex().Execute(store);
            }
        }
    }
}
