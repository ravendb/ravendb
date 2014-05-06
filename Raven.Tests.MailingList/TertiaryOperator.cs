// -----------------------------------------------------------------------
//  <copyright file="TertiaryOperator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class TertiaryOperator : RavenTest
    {
        [Fact]
        public void Breaks()
        {
           using (var store = NewDocumentStore())
           {
               store.ExecuteIndex(new BadIndex());
           }
        }

        [Fact]
        public void Works()
        {
            using (var store = NewDocumentStore())
            {
                store.ExecuteIndex(new GoodIndex());
            }
        }

        public class BadIndex : AbstractIndexCreationTask<Widget>
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

                TransformResults = (db, widgets) => from widget in widgets
                                                    let container = db.Load<Container>(widget.ContainerId)
                                                    select new
                                                    {
                                                        ContainedWidgetIds = container.ContainedWidgetIds[widget.Category] ?? new List<string>()
                                                    };
            }
        }

        public class Container
        {
            public string Id { get; set; }
            public Dictionary<string, List<string>> ContainedWidgetIds { get; set; }
        }

        public class GoodIndex : AbstractIndexCreationTask<Widget>
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

                TransformResults = (db, widgets) => from widget in widgets
                                                    let container = db.Load<Container>(widget.ContainerId)
                                                    select new
                                                    {
                                                        ContainedWidgetIds =
                                                        container.ContainedWidgetIds[widget.Category]
                                                    };
            }
        }

        public class Widget
        {
            public string Id { get; set; }
            public string ContainerId { get; set; }
            public string Category { get; set; }
        }
    }
}