// -----------------------------------------------------------------------
//  <copyright file="Kijana.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Kijana : RavenTestBase
    {
        public Kijana(ITestOutputHelper output) : base(output)
        {
        }

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
