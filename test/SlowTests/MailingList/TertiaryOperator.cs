// -----------------------------------------------------------------------
//  <copyright file="TertiaryOperator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class TertiaryOperator : RavenTestBase
    {
        [Fact]
        public void Breaks()
        {
           using (var store = GetDocumentStore())
           {
               store.ExecuteIndex(new BadIndex());
               store.ExecuteTransformer(new BadTransformer());
           }
        }

        [Fact]
        public void Works()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new GoodIndex());
                store.ExecuteTransformer(new GoodTransformer());
            }
        }

        private class BadIndex : AbstractIndexCreationTask<Widget>
        {
            public BadIndex()
            {
                Map = widgets => from widget in widgets
                                 select new
                                 {
                                     widget.Id,
                                     widget.ContainerId,
                                     widget.Category
                                 };
            }
        }

        private class BadTransformer : AbstractTransformerCreationTask<Widget>
        {
            public BadTransformer()
            {
                TransformResults = widgets => from widget in widgets
                                                    let container = LoadDocument<Container>(widget.ContainerId)
                                                    select new
                                                    {
                                                        ContainedWidgetIds = container.ContainedWidgetIds[widget.Category] ?? new List<string>()
                                                    };
            }
        }

        private class Container
        {
            public string Id { get; set; }
            public Dictionary<string, List<string>> ContainedWidgetIds { get; set; }
        }

        private class GoodIndex : AbstractIndexCreationTask<Widget>
        {
            public GoodIndex()
            {
                Map = widgets => from widget in widgets
                                 select new
                                 {
                                     widget.Id,
                                     widget.ContainerId,
                                     widget.Category
                                 };
            }
        }

        private class GoodTransformer : AbstractTransformerCreationTask<Widget>
        {
            public GoodTransformer()
            {
                TransformResults = widgets => from widget in widgets
                                                    let container = LoadDocument<Container>(widget.ContainerId)
                                                    select new
                                                    {
                                                        ContainedWidgetIds =
                                                        container.ContainedWidgetIds[widget.Category]
                                                    };
            }
        }

        private class Widget
        {
            public string Id { get; set; }
            public string ContainerId { get; set; }
            public string Category { get; set; }
        }
    }
}
