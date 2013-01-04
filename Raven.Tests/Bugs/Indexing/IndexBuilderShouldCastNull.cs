using System;
using Raven.Client.Document;
using Raven.Client.Indexes;
using System.Linq;
using Xunit;

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

		[Fact]
		public void NullableIndexDoesNotCastTwice()
		{
			var indexDefinition = new NullableIndex { Conventions = new DocumentConvention() }.CreateIndexDefinition();
			Assert.DoesNotContain("(string)(string)", indexDefinition.Map, StringComparison.OrdinalIgnoreCase); // Include also (String)(string)|(string)(String) cases.
			Assert.DoesNotContain("(System.String)(string)", indexDefinition.Map);
			Assert.DoesNotContain("(string)(System.String)", indexDefinition.Map);
			Assert.DoesNotContain("(System.String)(System.String)", indexDefinition.Map);
		}

		[Fact]
		public void AnonymousNullableIndexDoesNotCastTwice()
		{
			var indexDefinition = new AnonymousNullableIndex { Conventions = new DocumentConvention() }.CreateIndexDefinition();
			Assert.DoesNotContain("(string)(string)", indexDefinition.Map, StringComparison.OrdinalIgnoreCase); // Include also (String)(string)|(string)(String) cases.
			Assert.DoesNotContain("(System.String)(string)", indexDefinition.Map);
			Assert.DoesNotContain("(string)(System.String)", indexDefinition.Map);
			Assert.DoesNotContain("(System.String)(System.String)", indexDefinition.Map);
		}

		private abstract class Nullable
		{
			public char? Char { get; set; }
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
			public TimeSpan? TimeSpan { get; set; }
			public Guid? Guid { get; set; }
		}

		private class NullableIndex : AbstractMultiMapIndexCreationTask<NullableIndex.Result>
		{
			public class Result : Nullable
			{
			}

			public NullableIndex()
			{
				AddMap<Nullable>(nullables => nullables.Select(nullable => new Result
				{
					Char = null,
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
					TimeSpan = null,
					Guid = null,
				}));
			}
		}

		private class AnonymousNullableIndex : AbstractMultiMapIndexCreationTask<AnonymousNullableIndex.Result>
		{
			public class Result : Nullable
			{
			}

			public AnonymousNullableIndex()
			{
				AddMap<Nullable>(nullables => nullables.Select(nullable => new
				{
					Char = (char?)null,
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
					TimeSpan = (TimeSpan?)null,
					Guid = (Guid?)null,
				}));
			}
		}
	}
}