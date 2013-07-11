// -----------------------------------------------------------------------
//  <copyright file="MarkReadOnly.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Raven.Tests.MailingList
{
	public class MarkReadOnly : RavenTest
	{
		public class Widget
		{
			public string Id { get; set; }
			public string Property { get; set; }
		}

		[Fact]
		public void MarkReadOnlyIsNotPersistent()
		{
			const string widgetId = "widgets/1";

			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var entity = new Widget() { Id = widgetId, Property = "A" };
					session.Store(entity);
					session.SaveChanges();
				}

				// mark readonly
				using (var session = store.OpenSession())
				{
					var entity = session.Load<Widget>(widgetId);
					session.Advanced.MarkReadOnly(entity);
					session.SaveChanges();
				}

				// try changing
				using (var session = store.OpenSession())
				{
					var entity = session.Load<Widget>(widgetId);
					entity.Property = "B";

					Assert.True(session.Advanced.HasChanged(entity));
				}
			}
		}
	}
}