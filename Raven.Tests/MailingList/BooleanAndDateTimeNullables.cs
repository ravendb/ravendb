using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class BooleanAndDateTimeNullables : RavenTest
	{
		public class ObjectWithNullables
		{
			public ObjectWithNullables()
			{
				TimeCollection = new List<DateTimeOffset>();
			}

			public string Name { get; set; }
			public bool? Excluded { get; set; }
			public DateTimeOffset? Time { get; set; }
			public ICollection<DateTimeOffset> TimeCollection { get; set; }
		}

		public class Raven20Style_NullableBoolean : AbstractIndexCreationTask<ObjectWithNullables>
		{
			public Raven20Style_NullableBoolean()
			{
				Map = objects => from o in objects
								where !(o.Excluded ?? false)
								select new
										{
											o.Name
										};
			}
		}

		public class Raven25Style_NullableBoolean : AbstractIndexCreationTask<ObjectWithNullables>
		{
			public Raven25Style_NullableBoolean()
			{
				Map = objects => from o in objects
								where ((o.Excluded ?? false) == false)
								select new
										{
											o.Name
										};
			}
		}


		public class Raven20Style_NullableDateTimeOffset : AbstractIndexCreationTask<ObjectWithNullables>
		{
			public Raven20Style_NullableDateTimeOffset()
			{
				Map = objects => from o in objects
								select new
										{
											Times = o.TimeCollection.Any() ? o.TimeCollection.OrderByDescending(d => d).ToList() : new List<DateTimeOffset> { o.Time ?? DateTimeOffset.MinValue },
										};
			}
		}

		public class Raven25Style_NullableDateTimeOffset : AbstractIndexCreationTask<ObjectWithNullables>
		{
			public Raven25Style_NullableDateTimeOffset()
			{
				Map = objects => from o in objects
								select new
										{
											Times = o.TimeCollection.Any() ? o.TimeCollection.OrderByDescending(d => d).ToList() : new List<DateTimeOffset> {(DateTimeOffset)(o.Time ?? DateTimeOffset.MinValue) },
										};
			}
		}


		private void TestIndexSetup(params AbstractIndexCreationTask<ObjectWithNullables>[] indexes)
		{
			using (var store = NewDocumentStore())
			{
				foreach (var index in indexes)
				{
					index.Execute(store);
				}
			}			
		}

		[Fact]
		public void CanUseRaven20BoolIndex()
		{
			TestIndexSetup(new Raven20Style_NullableBoolean());
		}

		[Fact]
		public void CanUseRaven25BoolIndex()
		{
			TestIndexSetup(new Raven25Style_NullableBoolean());
		}

		[Fact]
		public void CanUseRaven20DateTimeIndex()
		{
			TestIndexSetup(new Raven20Style_NullableDateTimeOffset());
		}

		[Fact]
		public void CanUseRaven25DateTimeIndex()
		{
			TestIndexSetup(new Raven25Style_NullableDateTimeOffset());
		}

	}
}
