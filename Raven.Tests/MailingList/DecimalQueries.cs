using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using Xunit;
using System.Linq;
using Xunit.Extensions;

namespace Raven.Tests.MailingList
{
	public class DecimalQueries : RavenTest
	{
		public class Money
		{
			public decimal Amount { get; set; }
		}

		[Fact]
		public void CanQuery()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Money
					{
						Amount = 10.00m
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var moneys = session.Query<Money>()
						.Where(x => x.Amount == 10.000m)
						.ToArray();

					Assert.NotEmpty(moneys);
				}
			}
		}


		[Theory]
		[PropertyData("Cultures")]
		public void CanQueryWithOtherCulture(CultureInfo culture)
		{
			var oldCulture = Thread.CurrentThread.CurrentCulture;

			try
			{
				Thread.CurrentThread.CurrentCulture = culture;

				using (var store = NewDocumentStore())
				{
					using (var session = store.OpenSession())
					{
						session.Store(new Money
						{
							Amount = 12.34m
						});
						session.SaveChanges();
					}

					using (var session = store.OpenSession())
					{
						var moneys = session.Query<Money>()
							.Where(x => x.Amount == 12.34m)
							.ToArray();

						Assert.NotEmpty(moneys);
					}
				}
			}
			finally
			{
				Thread.CurrentThread.CurrentCulture = oldCulture;
			}
		}

		public static IEnumerable<object[]> Cultures
		{
			get
			{
				var cultures = new[]
				{ 
					CultureInfo.InvariantCulture,
					CultureInfo.CurrentCulture,
					CultureInfo.GetCultureInfo("NL"), // Uses comma instead of point: 12,34
					CultureInfo.GetCultureInfo("tr-TR"), // "The Turkey Test"
				};
				return cultures.Select(c => new object[] { c });
			}
		}

	}
}