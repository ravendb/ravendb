using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class WithDecimalValue : RavenTest
	{
		public class Item
		{
			public decimal Value { get; set; }
		}
		public class Dec : AbstractIndexCreationTask<Item>
		{
			public Dec()
			{
				Map = items => from item in items
				               select new {A = item.Value*0.83M};
			}
		}

		[Fact]
		public void CanCreate()
		{
			using(var store = NewDocumentStore())
			{
				new Dec().Execute(store);
			}
		}

		[Fact]
		public void IgnoresLocale()
		{
			var oldCurrentCulture = Thread.CurrentThread.CurrentCulture;
			Thread.CurrentThread.CurrentCulture = new CultureInfo("de");
			using (new DisposableAction(() =>
			{
				Thread.CurrentThread.CurrentCulture = oldCurrentCulture;
			}))
			{
				using (var store = NewDocumentStore())
				{
					new Dec().Execute(store);
				}
			}
		}
	}
}