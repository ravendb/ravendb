// -----------------------------------------------------------------------
//  <copyright file="SelectManyShouldWork.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;


//using System;
//using System.Collections.Generic;
//using System.Linq;
//using Raven.Server.Documents.Indexes.Static;
//using Raven.Server.Documents.Indexes.Static.Linq;
//using Raven.Server.Documents.Indexes.Static.Extensions;

//namespace Raven.Server.Documents.Indexes.Static.Generated
//{
//    public class Index_Creatives_ClickActions_1 : StaticIndexBase
//    {
//        public Index_Creatives_ClickActions_1()
//        {
//            this.AddMap("DroneStateSnapshoots", docs => docs.SelectMany(x => (IEnumerable<dynamic>)x.ClickActions, (snapshoot, x) => new
//            {
//                ClickedBy = new string[] { x.ContactId },
//                CreativeId = x.CreativeId
//            }

//            ));
//            this.Reduce = results => results.GroupBy(x => x.CreativeId).Select(x => new
//            {
//                ClickedBy = Enumerable.ToArray(x.SelectMany((Func<dynamic, IEnumerable<dynamic>>)(m => m.ClickedBy))),
//                CreativeId = x.Key
//            }

//            );
//            this.GroupByFields = new string[] { "CreativeId" };
//            this.OutputFields = new string[] { "ClickedBy", "CreativeId" };
//        }
//    }
//}

namespace SlowTests.Tests.Linq
{
    public class SelectManyShouldWork : RavenTestBase
    {
        private void Fill(DocumentStore store)
        {
            var snapshots = new[]
            {
                new DroneStateSnapshoot
                {
                    ClickActions = new List<ClickAction>
                    {
                        new ClickAction {ContactId = "contact/1", CreativeId = "creative/1"},
                        new ClickAction {ContactId = "contact/2", CreativeId = "creative/1"}
                    }
                },
                new DroneStateSnapshoot
                {
                    ClickActions = new List<ClickAction>
                    {
                        new ClickAction {ContactId = "contact/100", CreativeId = "creative/1"},
                        new ClickAction {ContactId = "contact/200", CreativeId = "creative/1"}
                    }
                },
                new DroneStateSnapshoot
                {
                    ClickActions = new List<ClickAction>
                    {
                        new ClickAction {ContactId = "contact/1000", CreativeId = "creative/2"},
                        new ClickAction {ContactId = "contact/2000", CreativeId = "creative/2"}
                    }
                },
                new DroneStateSnapshoot
                {
                    ClickActions = new List<ClickAction>
                    {
                        new ClickAction {ContactId = "contact/4000", CreativeId = "creative/2"},
                        new ClickAction {ContactId = "contact/5000", CreativeId = "creative/2"}
                    }
                }
            }.ToList();

            using (var session = store.OpenSession())
            {
                snapshots.ForEach(session.Store);
                session.SaveChanges();
            }
        }

        [Fact]
        public async Task SelectMany1_Works()
        {
            await AssertAgainstIndex<Creatives_ClickActions_1>();
        }

        [Fact]
        public async Task SelectMany2_ShouldWork()
        {
            await AssertAgainstIndex<Creatives_ClickActions_2>();
        }

        private async Task AssertAgainstIndex<TIndex>() where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                new TIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<ReduceResult, TIndex>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .ToList();

                    var indexingErrors = store
                        .DatabaseCommands
                        .GetIndexErrors()
                        .SelectMany(x => x.Errors)
                        .ToList();
                    Assert.Empty(indexingErrors);

                    Assert.Equal(2, result.Count);
                    Assert.Equal("creative/1", result.First().CreativeId);
                    Assert.Equal("creative/2", result.Last().CreativeId);
                }
            }
        }

        private class DroneStateSnapshoot
        {
            public IList<ClickAction> ClickActions { get; set; }
        }

        private class ClickAction
        {
            public string ContactId { get; set; }
            public string CreativeId { get; set; }
            public DateTime Date { get; set; }
        }

        private class ReduceResult
        {
            public string CreativeId { get; set; }
            public string[] ClickedBy { get; set; }
        }

        private class Creatives_ClickActions_1 : AbstractIndexCreationTask<DroneStateSnapshoot, ReduceResult>
        {
            public Creatives_ClickActions_1()
            {
                Map = snapshots => snapshots
                                       .SelectMany(x => x.ClickActions, (snapshoot, x) => new
                                       {
                                           ClickedBy = new[] { x.ContactId },
                                           x.CreativeId
                                       });

                Reduce = result => result
                                       .GroupBy(x => x.CreativeId)
                                       .Select(x => new
                                       {
                                           ClickedBy = x.SelectMany(m => m.ClickedBy).ToArray(),
                                           CreativeId = x.Key
                                       });
            }
        }

        private class Creatives_ClickActions_2 : AbstractIndexCreationTask<DroneStateSnapshoot, ReduceResult>
        {
            public Creatives_ClickActions_2()
            {
                Map = snapshots => snapshots
                                       .SelectMany(x => x.ClickActions)
                                       .Select(x => new
                                       {
                                           ClickedBy = new[] { x.ContactId },
                                           x.CreativeId
                                       });

                Reduce = result => result
                                       .GroupBy(x => x.CreativeId)
                                       .Select(x => new
                                       {
                                           ClickedBy = x.SelectMany(m => m.ClickedBy).ToArray(),
                                           CreativeId = x.Key
                                       });
            }
        }
    }
}
