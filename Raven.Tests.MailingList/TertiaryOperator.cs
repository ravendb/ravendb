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
			   store.ExecuteTransformer(new BadTransformer());
           }
        }

        [Fact]
        public void Works()
        {
            using (var store = NewDocumentStore())
            {
                store.ExecuteIndex(new GoodIndex());
				store.ExecuteTransformer(new GoodTransformer());
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
            }
        }

		public class BadTransformer : AbstractTransformerCreationTask<Widget>
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
            }
        }

		public class GoodTransformer : AbstractTransformerCreationTask<Widget>
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

        public class Widget
        {
            public string Id { get; set; }
            public string ContainerId { get; set; }
            public string Category { get; set; }
        }
    }
}