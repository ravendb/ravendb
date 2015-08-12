using System;
using System.Linq;
using Raven.Database.TimeSeries;
using Voron;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class RollupsRanges : TimeSeriesTest
	{
		[Fact]
		public void ByDays()
		{
			using (var tss = GetStorage())
			{
				var start = new DateTime(2015, 4, 6, 0, 0, 0);

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
					var time = r.QueryRollup(
						new TimeSeriesRollupQuery
						{
							Type = "Simple",
							Key = "Time",
							Start = start.AddYears(-1),
							End = start.AddYears(2),
							Duration = PeriodDuration.Days(2),
						}).ToArray();

					Assert.Equal(548, time.Length);
					for (int i = 0; i < 548; i++)
					{
#if DEBUG
						Assert.Equal("Time", time[i].DebugKey);
#endif
						Assert.Equal(PeriodDuration.Days(2), time[i].Duration);

						var daysInMonth = DateTime.DaysInMonth(time[i].StartAt.Year, time[i].StartAt.Month) +
										  DateTime.DaysInMonth(time[i].StartAt.AddMonths(1).Year, time[i].StartAt.AddMonths(1).Month);
						if (i == 182)
						{
							Assert.Equal(710, time[i].Value.Volume);
							Assert.Equal(258795, time[i].Value.Sum);
							Assert.Equal(10, time[i].Value.Open);
							Assert.Equal(10, time[i].Value.Low);
							Assert.Equal(719, time[i].Value.Close);
							Assert.Equal(719, time[i].Value.High);
						}
						else if (i == 183)
						{
							Assert.Equal(1280, time[i].Value.Volume);
							Assert.Equal(1740160, time[i].Value.Sum);
							Assert.Equal(720, time[i].Value.Open);
							Assert.Equal(720, time[i].Value.Low);
							Assert.Equal(1999, time[i].Value.Close);
							Assert.Equal(1999, time[i].Value.High);
						}
						else
						{
							Assert.Equal(0, time[i].Value.Volume);
							Assert.Equal(0, time[i].Value.Sum);
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
				var start = new DateTime(2015, 4, 1, 0, 0, 0);

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
					var time = r.QueryRollup(
						new TimeSeriesRollupQuery
						{
							Type = "Simple",
							Key = "Time",
							Start = start.AddYears(-1),
							End = start.AddYears(2),
							Duration = PeriodDuration.Months(2),
						}).ToArray();

					Assert.Equal(3 * 12 / 2, time.Length);
					Assert.Equal(new DateTime(2014, 4, 1, 0, 0, 0), time[0].StartAt);
					Assert.Equal(new DateTime(2014, 6, 1, 0, 0, 0), time[1].StartAt);
					Assert.Equal(new DateTime(2014, 8, 1, 0, 0, 0), time[2].StartAt);
					Assert.Equal(new DateTime(2014, 10, 1, 0, 0, 0), time[3].StartAt);
					Assert.Equal(new DateTime(2014, 12, 1, 0, 0, 0), time[4].StartAt);
					Assert.Equal(new DateTime(2015, 2, 1, 0, 0, 0), time[5].StartAt);
					Assert.Equal(new DateTime(2015, 4, 1, 0, 0, 0), time[6].StartAt);
					Assert.Equal(new DateTime(2015, 6, 1, 0, 0, 0), time[7].StartAt);
					Assert.Equal(new DateTime(2015, 8, 1, 0, 0, 0), time[8].StartAt);
					Assert.Equal(new DateTime(2015, 10, 1, 0, 0, 0), time[9].StartAt);
					Assert.Equal(new DateTime(2015, 12, 1, 0, 0, 0), time[10].StartAt);
					Assert.Equal(new DateTime(2016, 2, 1, 0, 0, 0), time[11].StartAt);
					Assert.Equal(new DateTime(2016, 4, 1, 0, 0, 0), time[12].StartAt);
					Assert.Equal(new DateTime(2016, 6, 1, 0, 0, 0), time[13].StartAt);
					Assert.Equal(new DateTime(2016, 8, 1, 0, 0, 0), time[14].StartAt);
					Assert.Equal(new DateTime(2016, 10, 1, 0, 0, 0), time[15].StartAt);
					Assert.Equal(new DateTime(2016, 12, 1, 0, 0, 0), time[16].StartAt);
					Assert.Equal(new DateTime(2017, 2, 1, 0, 0, 0), time[17].StartAt);

					for (int i = 0; i < 18; i++)
					{
#if DEBUG
						Assert.Equal("Time", time[i].DebugKey);
#endif
						Assert.Equal(PeriodDuration.Months(2), time[i].Duration);

						var daysInMonth = DateTime.DaysInMonth(time[i].StartAt.Year, time[i].StartAt.Month) +
										  DateTime.DaysInMonth(time[i].StartAt.AddMonths(1).Year, time[i].StartAt.AddMonths(1).Month);
						if (i == 6)
						{
							Assert.Equal(daysInMonth * 4 - 2.5 * 4, time[i].Value.Volume);
							Assert.NotEqual(0, time[i].Value.Sum);
						}
						else if (i > 6)
						{
							Assert.Equal(daysInMonth * 4, time[i].Value.Volume);
							Assert.NotEqual(0, time[i].Value.Sum);
							Assert.NotEqual(0, time[i].Value.High);
							Assert.NotEqual(0, time[i].Value.Low);
							Assert.NotEqual(0, time[i].Value.Open);
							Assert.NotEqual(0, time[i].Value.Close);
						}
						else
						{
							Assert.Equal(0, time[i].Value.Volume);
							Assert.Equal(0, time[i].Value.Sum);
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
				var start = new DateTime(2014, 1, 1, 0, 0, 0);

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
					var time = r.QueryRollup(
						new TimeSeriesRollupQuery
						{
							Type = "Simple",
							Key = "Time",
							Start = start.AddYears(-2),
							End = start.AddYears(6),
							Duration = PeriodDuration.Years(2),
						}).ToArray();

					Assert.Equal(8/2, time.Length);
					Assert.Equal(new DateTime(2012, 1, 1, 0, 0, 0), time[0].StartAt);
					Assert.Equal(new DateTime(2014, 1, 1, 0, 0, 0), time[1].StartAt);
					Assert.Equal(new DateTime(2016, 1, 1, 0, 0, 0), time[2].StartAt);
					Assert.Equal(new DateTime(2018, 1, 1, 0, 0, 0), time[3].StartAt);

					Assert.Equal(0, time[0].Value.Volume);
					Assert.Equal(0, time[0].Value.Sum);
					Assert.Equal(2910, time[1].Value.Volume);
					Assert.Equal(4261695, time[1].Value.Sum);
					Assert.Equal(2080, time[2].Value.Volume);
					Assert.Equal(8235760, time[2].Value.Sum);
					Assert.Equal(0, time[3].Value.Volume);
					Assert.Equal(0, time[3].Value.Sum);
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