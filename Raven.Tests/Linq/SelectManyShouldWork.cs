// -----------------------------------------------------------------------
//  <copyright file="SelectManyShouldWork.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Linq
{
	public class SelectManyShouldWork : RavenTest
	{
		private readonly EmbeddableDocumentStore store;

		public SelectManyShouldWork()
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

			store = NewDocumentStore();
			using (var session = store.OpenSession())
			{
				snapshots.ForEach(session.Store);
				session.SaveChanges();
			}
		}

		[Fact]
		public void SelectMany1_Works()
		{
			AssertAgainstIndex<Creatives_ClickActions_1>();
		}

		[Fact]
		public void SelectMany2_ShouldWork()
		{
			AssertAgainstIndex<Creatives_ClickActions_2>();
		}

		public void AssertAgainstIndex<TIndex>() where TIndex : AbstractIndexCreationTask, new()
		{
			new TIndex().Execute(store);

			using (var session = store.OpenSession())
			{
				var result = session.Query<ReduceResult, TIndex>()
				                    .Customize(customization => customization.WaitForNonStaleResults())
				                    .ToList();

				Assert.Empty(store.DatabaseCommands.GetStatistics().Errors);

				Assert.Equal(2, result.Count);
				Assert.Equal("creative/1", result.First().CreativeId);
				Assert.Equal("creative/2", result.Last().CreativeId);
			}
		}

		public class DroneStateSnapshoot
		{
			public IList<ClickAction> ClickActions { get; set; }
		}

		public class ClickAction
		{
			public string ContactId { get; set; }
			public string CreativeId { get; set; }
			public DateTime Date { get; set; }
		}

		public class ReduceResult
		{
			public string CreativeId { get; set; }
			public string[] ClickedBy { get; set; }
		}

		public class Creatives_ClickActions_1 : AbstractIndexCreationTask<DroneStateSnapshoot, ReduceResult>
		{
			public Creatives_ClickActions_1()
			{
				Map = snapshots => snapshots
					                   .SelectMany(x => x.ClickActions, (snapshoot, x) => new
					                   {
						                   ClickedBy = new[] {x.ContactId},
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

		public class Creatives_ClickActions_2 : AbstractIndexCreationTask<DroneStateSnapshoot, ReduceResult>
		{
			public Creatives_ClickActions_2()
			{
				Map = snapshots => snapshots
					                   .SelectMany(x => x.ClickActions)
					                   .Select(x => new
					                   {
						                   ClickedBy = new[] {x.ContactId},
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