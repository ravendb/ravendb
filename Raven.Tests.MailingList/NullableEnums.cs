// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class NullableEnums : RavenTest
	{
		#region MonitorCategory enum

		public enum MonitorCategory
		{
			Normal,
			WideScreen
		}

		#endregion

		[Fact]
		public void CanQueryByEnum()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new ModelWithEnum
					{
						Category =
							MonitorCategory.Normal
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var fromDb = session.Query<ModelWithEnum>().FirstOrDefault(m =>
					                                                           m.Category == MonitorCategory.Normal);
					Assert.NotNull(fromDb);
				}
			}
		}

		[Fact]
		public void CanQueryByNullableEnum()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new ModelWithEnum
					{
						NullableCategory = MonitorCategory.Normal
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var fromDb = session.Query<ModelWithEnum>()
						.FirstOrDefault(m => m.NullableCategory == MonitorCategory.Normal);
					Assert.NotNull(fromDb);
				}
			}
		}

		[Fact]
		public void CanQueryByNullableEnumThatIsNull()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new ModelWithEnum {NullableCategory = null});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var fromDb = session.Query<ModelWithEnum>().FirstOrDefault(m =>
					                                                           m.NullableCategory == null);
					Assert.NotNull(fromDb);
				}
			}
		}

		#region Nested type: ModelWithEnum

		public class ModelWithEnum
		{
			public MonitorCategory Category { get; set; }
			public MonitorCategory? NullableCategory { get; set; }
		}

		#endregion
	}
}