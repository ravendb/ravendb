// -----------------------------------------------------------------------
//  <copyright file="Kijana.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class Kijana : RavenTest
    {
        public class Scratch
        {
            public string Id { get; set; }
            public long Value { get; set; }
        }

        class ScratchIndex : AbstractIndexCreationTask<Scratch>
        {
            public ScratchIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        doc.Value
                    };

                Sort(x => x.Value, SortOptions.Long);
            }
        }

        [Fact]
        public void CanSetSortValue()
        {
            using (var store = NewRemoteDocumentStore())
            {
                new ScratchIndex().Execute(store);
            }
        }
    }
}
