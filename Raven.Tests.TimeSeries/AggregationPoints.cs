using System;
using System.Linq;
using Raven.Abstractions.TimeSeries;
using Raven.Database.TimeSeries;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class AggregationPoints : TimeSeriesTest
	{
		[Fact]
		public void ByDays()
		{
			using (var tss = GetStorage())
			{
				var start = new DateTimeOffset(2015, 4, 6, 0, 0, 0, TimeSpan.Zero);

				using (var writer = tss.CreateWriter())
				{
					for (int i = 10; i < 2000; i++)
					{
						var dateTime = start.AddMinutes(2 * i);
						writer.Append("Simple", "Time", dateTime, i);
					}
					writer.Commit();
				}

				using (var r = tss.CreateReader())
				{
					var time = r.GetAggregatedPoints("Simple", "Time", AggregationDuration.Days(2), start.AddYears(-1), start.AddYears(2)).ToArray();

					Assert.Equal(548, time.Length);
					for (int i = 0; i < 548; i++)
					{
#if DEBUG
						Assert.Equal("Time", time[i].DebugKey);
#endif
						Assert.Equal(AggregationDuration.Days(2), time[i].Duration);

						var daysInMonth = DateTime.DaysInMonth(time[i].StartAt.Year, time[i].StartAt.Month) +
										  DateTime.DaysInMonth(time[i].StartAt.AddMonths(1).Year, time[i].StartAt.AddMonths(1).Month);
						if (i == 182)
						{
							Assert.Equal(710, time[i].Values[0].Volume);
							Assert.Equal(258795, time[i].Values[0].Sum);
							Assert.Equal(10, time[i].Values[0].Open);
							Assert.Equal(10, time[i].Values[0].Low);
							Assert.Equal(719, time[i].Values[0].Close);
							Assert.Equal(719, time[i].Values[0].High);
						}
						else if (i == 183)
						{
							Assert.Equal(1280, time[i].Values[0].Volume);
							Assert.Equal(1740160, time[i].Values[0].Sum);
							Assert.Equal(720, time[i].Values[0].Open);
							Assert.Equal(720, time[i].Values[0].Low);
							Assert.Equal(1999, time[i].Values[0].Close);
							Assert.Equal(1999, time[i].Values[0].High);
						}
						else
						{
							Assert.Equal(0, time[i].Values[0].Volume);
							Assert.Equal(0, time[i].Values[0].Sum);
						}
					}
				}
			}
		}

		[Fact]
		public void ByMonths()
		{
			using (var tss = GetStorage())
			{
				var start = new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero);

				using (var writer = tss.CreateWriter())
				{
					for (int i = 10; i < 5000; i++)
					{
						var dateTime = start.AddHours(6 * i);
						writer.Append("Simple", "Time", dateTime, i);
					}
					writer.Commit();
				}

				using (var r = tss.CreateReader())
				{
					var time = r.GetAggregatedPoints("Simple", "Time", AggregationDuration.Months(2), start.AddYears(-1), start.AddYears(2)).ToArray();

					Assert.Equal(3 * 12 / 2, time.Length);
					Assert.Equal(new DateTimeOffset(2014, 4, 1, 0, 0, 0, TimeSpan.Zero), time[0].StartAt);
					Assert.Equal(new DateTimeOffset(2014, 6, 1, 0, 0, 0, TimeSpan.Zero), time[1].StartAt);
					Assert.Equal(new DateTimeOffset(2014, 8, 1, 0, 0, 0, TimeSpan.Zero), time[2].StartAt);
					Assert.Equal(new DateTimeOffset(2014, 10, 1, 0, 0, 0, TimeSpan.Zero), time[3].StartAt);
					Assert.Equal(new DateTimeOffset(2014, 12, 1, 0, 0, 0, TimeSpan.Zero), time[4].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 2, 1, 0, 0, 0, TimeSpan.Zero), time[5].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero), time[6].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 6, 1, 0, 0, 0, TimeSpan.Zero), time[7].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 8, 1, 0, 0, 0, TimeSpan.Zero), time[8].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 10, 1, 0, 0, 0, TimeSpan.Zero), time[9].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 12, 1, 0, 0, 0, TimeSpan.Zero), time[10].StartAt);
					Assert.Equal(new DateTimeOffset(2016, 2, 1, 0, 0, 0, TimeSpan.Zero), time[11].StartAt);
					Assert.Equal(new DateTimeOffset(2016, 4, 1, 0, 0, 0, TimeSpan.Zero), time[12].StartAt);
					Assert.Equal(new DateTimeOffset(2016, 6, 1, 0, 0, 0, TimeSpan.Zero), time[13].StartAt);
					Assert.Equal(new DateTimeOffset(2016, 8, 1, 0, 0, 0, TimeSpan.Zero), time[14].StartAt);
					Assert.Equal(new DateTimeOffset(2016, 10, 1, 0, 0, 0, TimeSpan.Zero), time[15].StartAt);
					Assert.Equal(new DateTimeOffset(2016, 12, 1, 0, 0, 0, TimeSpan.Zero), time[16].StartAt);
					Assert.Equal(new DateTimeOffset(2017, 2, 1, 0, 0, 0, TimeSpan.Zero), time[17].StartAt);

					for (int i = 0; i < 18; i++)
					{
#if DEBUG
						Assert.Equal("Time", time[i].DebugKey);
#endif
						Assert.Equal(AggregationDuration.Months(2), time[i].Duration);

						var daysInMonth = DateTime.DaysInMonth(time[i].StartAt.Year, time[i].StartAt.Month) +
										  DateTime.DaysInMonth(time[i].StartAt.AddMonths(1).Year, time[i].StartAt.AddMonths(1).Month);
						if (i == 6)
						{
							Assert.Equal(daysInMonth * 4 - 2.5 * 4, time[i].Values[0].Volume);
							Assert.NotEqual(0, time[i].Values[0].Sum);
						}
						else if (i > 6)
						{
							Assert.Equal(daysInMonth * 4, time[i].Values[0].Volume);
							Assert.NotEqual(0, time[i].Values[0].Sum);
							Assert.NotEqual(0, time[i].Values[0].High);
							Assert.NotEqual(0, time[i].Values[0].Low);
							Assert.NotEqual(0, time[i].Values[0].Open);
							Assert.NotEqual(0, time[i].Values[0].Close);
						}
						else
						{
							Assert.Equal(0, time[i].Values[0].Volume);
							Assert.Equal(0, time[i].Values[0].Sum);
						}
					}
				}
			}
		}

		[Fact]
		public void ByYear()
		{
			using (var tss = GetStorage())
			{
				var start = new DateTimeOffset(2014, 1, 1, 0, 0, 0, TimeSpan.Zero);

				using (var writer = tss.CreateWriter())
				{
					for (int i = 10; i < 5000; i++)
					{
						var dateTime = start.AddHours(6 * i);
						writer.Append("Simple", "Time", dateTime, i);
					}
					writer.Commit();
				}

				using (var r = tss.CreateReader())
				{
					var time = r.GetAggregatedPoints("Simple", "Time", AggregationDuration.Years(2), start.AddYears(-2), start.AddYears(6)).ToArray();

					Assert.Equal(8/2, time.Length);
					Assert.Equal(new DateTimeOffset(2012, 1, 1, 0, 0, 0, TimeSpan.Zero), time[0].StartAt);
					Assert.Equal(new DateTimeOffset(2014, 1, 1, 0, 0, 0, TimeSpan.Zero), time[1].StartAt);
					Assert.Equal(new DateTimeOffset(2016, 1, 1, 0, 0, 0, TimeSpan.Zero), time[2].StartAt);
					Assert.Equal(new DateTimeOffset(2018, 1, 1, 0, 0, 0, TimeSpan.Zero), time[3].StartAt);

					Assert.Equal(0, time[0].Values[0].Volume);
					Assert.Equal(0, time[0].Values[0].Sum);
					Assert.Equal(2910, time[1].Values[0].Volume);
					Assert.Equal(4261695, time[1].Values[0].Sum);
					Assert.Equal(2080, time[2].Values[0].Volume);
					Assert.Equal(8235760, time[2].Values[0].Sum);
					Assert.Equal(0, time[3].Values[0].Volume);
					Assert.Equal(0, time[3].Values[0].Sum);
				}
			}
		}

		private int Factorial(int start, int stop)
		{
			var result = 0;
			for (var i = start; i <= stop; i++)
			{
				result += i;
			}
			return result;
		}
	}
}