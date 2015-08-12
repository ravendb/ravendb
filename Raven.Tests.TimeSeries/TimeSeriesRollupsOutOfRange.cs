using System;
using System.Linq;
using Raven.Database.TimeSeries;
using Voron;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesRollupsOutOfRange : TimeSeriesTest
	{
		[Fact]
		public void HourlyData_QueryPer3Hours_StartedAt4()
		{
			var start = new DateTime(2015, 4, 1, 4, 0, 0);

			using (var tss = GetStorage())
			{
				var r = tss.CreateReader();
				Assert.Throws<InvalidOperationException>(() =>
				{
					r.QueryRollup(new TimeSeriesRollupQuery
					{
						Type = "Simple",
						Key = "Time",
						Start = start.AddYears(-1),
						End = start.AddYears(1),
						Duration = PeriodDuration.Hours(3),
					}).ToArray();
				});
				
			}
		}

		[Fact]
		public void HourlyData_QueryPer2Hours_StartedAt9()
		{
			var start = new DateTime(2015, 4, 1, 9, 0, 0);

			using (var tss = GetStorage())
			{
				var r = tss.CreateReader();
				Assert.Throws<InvalidOperationException>(() =>
				{
					r.QueryRollup(new TimeSeriesRollupQuery
					{
						Type = "Simple",
						Key = "Time",
						Start = start.AddYears(-1),
						End = start.AddYears(1),
						Duration = PeriodDuration.Hours(2),
					}).ToArray();
				});

			}
		}

		[Fact]
		public void HourlyData_QueryPer3Months_StartedAt4()
		{
			var start = new DateTime(2015, 4, 1, 0, 0, 0);

			using (var tss = GetStorage())
			{
				var r = tss.CreateReader();
				Assert.Throws<InvalidOperationException>(() =>
				{
					r.QueryRollup(new TimeSeriesRollupQuery
					{
						Type = "Simple",
						Key = "Time",
						Start = start.AddYears(-1),
						End = start.AddYears(2),
						Duration = PeriodDuration.Months(3),
					}).ToArray();
				});
			}
		}

		[Fact]
		public void HourlyData_QueryPer2Years_StartedAt2013MiddleYear()
		{
			var start = new DateTime(2015, 4, 1, 0, 0, 0);

			using (var tss = GetStorage())
			{
				var r = tss.CreateReader();
				var exception = Assert.Throws<InvalidOperationException>(() =>
				{
					r.QueryRollup(new TimeSeriesRollupQuery
					{
						Type = "Simple",
						Key = "Time",
						Start = start.AddYears(-1),
						End = start.AddYears(2),
						Duration = PeriodDuration.Years(2),
					}).ToArray();
				});
				Assert.Equal("When querying a roll up by years, you cannot specify months, days, hours, minutes, seconds or milliseconds", exception.Message);
			}
		}

		[Fact]
		public void HourlyData_QueryPer2Years_StartedAt2013()
		{
			var start = new DateTime(2015, 1, 1, 0, 0, 0);

			using (var tss = GetStorage())
			{
				var r = tss.CreateReader();

				var exception = Assert.Throws<InvalidOperationException>(() =>
				{
					r.QueryRollup(new TimeSeriesRollupQuery
					{
						Type = "Simple",
						Key = "Time",
						Start = start.AddYears(-1),
						End = start.AddYears(2),
						Duration = PeriodDuration.Years(2),
					}).ToArray();
				});
				Assert.Equal("Cannot create a roll up by 2 Years as it cannot be divided to candles that ends in midnight", exception.Message);
			}
		}

		[Fact]
		public void HourlyData_QueryPer3Years_StartedAt2017()
		{
			var start = new DateTime(2019, 1, 1, 0, 0, 0);

			using (var tss = GetStorage())
			{
				var r = tss.CreateReader();

				var exception = Assert.Throws<InvalidOperationException>(() =>
				{
					r.QueryRollup(new TimeSeriesRollupQuery
					{
						Type = "Simple",
						Key = "Time",
						Start = start.AddYears(-2),
						End = start.AddYears(7),
						Duration = PeriodDuration.Years(3),
					}).ToArray();
				});
				Assert.Equal("Cannot create a roll up by 3 Years as it cannot be divided to candles that starts from midnight", exception.Message);
			}
		}
	}
}