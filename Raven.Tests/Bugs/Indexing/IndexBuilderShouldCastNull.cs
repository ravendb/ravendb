using System;
using Raven.Client.Indexes;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class IndexBuilderShouldCastNull : RavenTest
	{
		[Fact]
		public void ShouldCastNullToThePropertyType()
		{
			using (var store = NewDocumentStore())
			{
				new NullableIndex().Execute(store);
			}
		}

		[Fact]
		public void ShouldWorkAlsoWithAnonymousResultTypeWhichRequiredExplicitlyCast()
		{
			using (var store = NewDocumentStore())
			{
				new AnonymousNullableIndex().Execute(store);
			}
		}

		private abstract class Nullable
		{
			public string String { get; set; }
			public object Object { get; set; }
			public decimal? Decimal { get; set; }
			public double? Double { get; set; }
			public float? Float { get; set; }
			public long? Long { get; set; }
			public int? Int { get; set; }
			public short? Short { get; set; }
			public byte? Byte { get; set; }
			public bool? Bool { get; set; }
			public DateTime? DateTime { get; set; }
			public DateTimeOffset? DateTimeOffset { get; set; }
		}

		public class NullableIndex : AbstractMultiMapIndexCreationTask<Result>
		{
			private class Result : Nullable
			{
			}

			public NullableIndex()
			{
				AddMap<Nullable>(nullables => nullables.Select(nullable => new Result
				{
					String = null,
					Object = null,
					Decimal = null,
					Double = null,
					Float = null,
					Long = null,
					Int = null,
					Short = null,
					Byte = null,
					Bool = null,
					DateTime = null,
					DateTimeOffset = null,
				}));
			}
		}

		public class AnonymousNullableIndex : AbstractMultiMapIndexCreationTask<Result>
		{
			private class Result : Nullable
			{
			}

			public AnonymousNullableIndex()
			{
				AddMap<Nullable>(nullables => nullables.Select(nullable => new
				{
					String = (string)null,
					Object = (object)null,
					Decimal = (decimal?)null,
					Double = (double?)null,
					Float = (float?)null,
					Long = (long?)null,
					Int = (int?)null,
					Short = (short?)null,
					Byte = (byte?)null,
					Bool = (bool?)null,
					DateTime = (DateTime?)null,
					DateTimeOffset = (DateTimeOffset?)null,
				}));
			}
		}
	}
}