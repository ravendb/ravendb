using System;
using System.Globalization;
using System.Linq;
using Raven.Database.TimeSeries;
using Voron;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesTests : TimeSeriesTest
	{
		[Fact]
		public void CanQueryData()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.Query(
						new TimeSeriesQuery
						{
							Key = "Time",
							Start = start.AddYears(-1),
							End = start.AddYears(1),
						},
						new TimeSeriesQuery
						{
							Key = "Money",
							Start = DateTime.MinValue,
							End = DateTime.MaxValue
						});

					Assert.Equal(2, result.Count());
					var time = result.First().ToArray();
					var money = result.Last().ToArray();

					Assert.Equal(3, time.Length);
					Assert.Equal(new DateTime(2015, 4, 1, 0, 0, 0), time[0].At);
					Assert.Equal(new DateTime(2015, 4, 1, 1, 0, 0), time[1].At);
					Assert.Equal(new DateTime(2015, 4, 1, 2, 0, 0), time[2].At);
					Assert.Equal(10, time[0].Value);
					Assert.Equal(19, time[1].Value);
					Assert.Equal(50, time[2].Value);
					Assert.Equal("Time", time[0].DebugKey);
					Assert.Equal("Time", time[1].DebugKey);
					Assert.Equal("Time", time[2].DebugKey);
					
					Assert.Equal(3, money.Length);
					Assert.Equal("Money", money[0].DebugKey);
					Assert.Equal("Money", money[1].DebugKey);
					Assert.Equal("Money", money[2].DebugKey);
				}
			}
		}

		[Fact]
		public void CanQueryDataOnSeries()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				using (var r = tss.CreateReader())
				{
					var result = r.Query(
						new TimeSeriesQuery
						{
							Key = "Money",
							Start = DateTime.MinValue,
							End = DateTime.MaxValue
						});

					var money = result.ToArray();
					Assert.Equal(3, money.Length);
					Assert.Equal("Money", money[0].DebugKey);
					Assert.Equal("Money", money[1].DebugKey);
					Assert.Equal("Money", money[2].DebugKey);
				}
			}
		}

		[Fact]
		public void CanQueryDataInSpecificDurationSum()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.QueryRollup(
						new TimeSeriesRollupQuery
						{
							Key = "Time",
							Start = start.AddDays(-1),
							End = start.AddDays(2),
							Duration = PeriodDuration.Hours(6),
						},
						new TimeSeriesRollupQuery
						{
							Key = "Money",
							Start = start.AddDays(-2),
							End = start.AddDays(1),
							Duration = PeriodDuration.Hours(2),
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(12, time.Length);
					Assert.Equal(new DateTime(2015, 3, 31, 0, 0, 0), time[0].StartAt);
					Assert.Equal(new DateTime(2015, 3, 31, 6, 0, 0), time[1].StartAt);
					Assert.Equal(new DateTime(2015, 3, 31, 12, 0, 0), time[2].StartAt);
					Assert.Equal(new DateTime(2015, 3, 31, 18, 0, 0), time[3].StartAt);
					Assert.Equal(new DateTime(2015, 4, 1, 0, 0, 0), time[4].StartAt);
					Assert.Equal(new DateTime(2015, 4, 1, 6, 0, 0), time[5].StartAt);
					Assert.Equal(new DateTime(2015, 4, 1, 12, 0, 0), time[6].StartAt);
					Assert.Equal(new DateTime(2015, 4, 1, 18, 0, 0), time[7].StartAt);
					Assert.Equal(new DateTime(2015, 4, 2, 0, 0, 0), time[8].StartAt);
					Assert.Equal(new DateTime(2015, 4, 2, 6, 0, 0), time[9].StartAt);
					Assert.Equal(new DateTime(2015, 4, 2, 12, 0, 0), time[10].StartAt);
					Assert.Equal(new DateTime(2015, 4, 2, 18, 0, 0), time[11].StartAt);
					Assert.Equal(79, time[4].Sum);
					Assert.Equal("Time", time[4].DebugKey);
					Assert.Equal(PeriodDuration.Hours(6), time[4].Duration);

					Assert.Equal(36, money.Length);
					Assert.Equal("Money", money[0].DebugKey);
					Assert.Equal("Money", money[24].DebugKey);
					Assert.Equal("Money", money[25].DebugKey);
					Assert.Equal(0, money[0].Sum);
					Assert.Equal(600, money[24].Sum);
					Assert.Equal(130, money[25].Sum);
					Assert.Equal(PeriodDuration.Hours(2), money[0].Duration);
					Assert.Equal(PeriodDuration.Hours(2), money[24].Duration);
					Assert.Equal(PeriodDuration.Hours(2), money[25].Duration);
				}
			}
		}

		[Fact]
		public void CanQueryDataInSpecificDurationAverage()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.QueryRollup(
						new TimeSeriesRollupQuery
						{
							Key = "Time",
							Start = start.AddMonths(-1),
							End = start.AddDays(1),
							Duration = PeriodDuration.Hours(3),
						},
						new TimeSeriesRollupQuery
						{
							Key = "Money",
							Start = start.AddDays(-1),
							End = start.AddMonths(1),
							Duration = PeriodDuration.Hours(2),
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(24 / 3 * 32, time.Length);
					for (int i = 0; i < 248; i++)
					{
						Assert.Equal(0, time[i].Volume);
					}
					for (int i = 249; i < 256; i++)
					{
						Assert.Equal(0, time[i].Volume);
					}
					Assert.Equal("26.3333333333333", (time[248].Sum / time[248].Volume).ToString(CultureInfo.InvariantCulture));
					Assert.Equal("Time", time[248].DebugKey);
					Assert.Equal(PeriodDuration.Hours(3), time[248].Duration);
					Assert.Equal(3, time[248].Volume);
					Assert.Equal(10, time[248].Open);
					Assert.Equal(50, time[248].Close);
					Assert.Equal(50, time[248].High);
					Assert.Equal(10, time[248].Low);


					Assert.Equal(24 / 2 * 31, money.Length);
					for (int i = 0; i < 372; i++)
					{
						if (i == 12 || i == 13)
							continue;
						Assert.Equal(0, money[i].Volume);
					}
					Assert.Equal("Money", money[12].DebugKey);
					Assert.Equal(300, money[12].Sum / money[12].Volume);
					Assert.Equal(130, money[13].Sum / money[13].Volume);
					Assert.Equal(PeriodDuration.Hours(2), money[12].Duration);
					Assert.Equal(PeriodDuration.Hours(2), money[13].Duration);
					Assert.Equal(2, money[12].Volume);
					Assert.Equal(1, money[13].Volume);
					Assert.Equal(54, money[12].Open);
					Assert.Equal(130, money[13].Open);
					Assert.Equal(546, money[12].Close);
					Assert.Equal(130, money[13].Close);
					Assert.Equal(54, money[12].Low);
					Assert.Equal(130, money[13].Low);
					Assert.Equal(546, money[12].High);
					Assert.Equal(130, money[13].High);
				}
			}
		}

		[Fact]
		public void CanQueryDataInSpecificDurationAverageUpdateRollup()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.QueryRollup(
						new TimeSeriesRollupQuery
						{
							Key = "Time",
							Start = start.AddMonths(-1),
							End = start.AddDays(1),
							Duration = PeriodDuration.Hours(3),
						},
						new TimeSeriesRollupQuery
						{
							Key = "Money",
							Start = start.AddDays(-1),
							End = start.AddMonths(1),
							Duration = PeriodDuration.Hours(2),
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(24/3*32, time.Length);
					for (int i = 0; i < 256; i++)
					{
						if (i == 248)
							continue;
						Assert.Equal(0, time[i].Volume);
					}
					Assert.Equal("26.3333333333333", (time[248].Sum/time[248].Volume).ToString(CultureInfo.InvariantCulture));
					Assert.Equal("Time", time[248].DebugKey);
					Assert.Equal(PeriodDuration.Hours(3), time[248].Duration);
					Assert.Equal(3, time[248].Volume);
					Assert.Equal(10, time[248].Open);
					Assert.Equal(50, time[248].Close);
					Assert.Equal(50, time[248].High);
					Assert.Equal(10, time[248].Low);

					Assert.Equal(24/2*31, money.Length);
					for (int i = 0; i < 372; i++)
					{
						if (i == 12 || i == 13)
							continue;
						Assert.Equal(0, money[i].Volume);
					}
					Assert.Equal("Money", money[12].DebugKey);
					Assert.Equal(300, money[12].Sum/money[12].Volume);
					Assert.Equal(130, money[13].Sum/money[13].Volume);
					Assert.Equal(PeriodDuration.Hours(2), money[12].Duration);
					Assert.Equal(PeriodDuration.Hours(2), money[13].Duration);
					Assert.Equal(2, money[12].Volume);
					Assert.Equal(1, money[13].Volume);
					Assert.Equal(54, money[12].Open);
					Assert.Equal(130, money[13].Open);
					Assert.Equal(546, money[12].Close);
					Assert.Equal(130, money[13].Close);
					Assert.Equal(54, money[12].Low);
					Assert.Equal(130, money[13].Low);
					Assert.Equal(546, money[12].High);
					Assert.Equal(130, money[13].High);
				}

				var start2 = start.AddMonths(-1).AddDays(5);
				using (var writer = tss.CreateWriter(SeriesType.Simple()))
				{
					int value = 6;
					for (int i = 0; i < 4; i++)
					{
						writer.Append("Time", start2.AddHours(2 + i), value++);
						writer.Append("Is", start2.AddHours(3 + i), value++);
						writer.Append("Money", start2.AddHours(3 + i), value++);
					}
					writer.Commit();
				}

				using (var r = tss.CreateReader())
				{
					var result = r.QueryRollup(
						new TimeSeriesRollupQuery
						{
							Key = "Time",
							Start = start.AddMonths(-1),
							End = start.AddDays(1),
							Duration = PeriodDuration.Hours(3),
						},
						new TimeSeriesRollupQuery
						{
							Key = "Money",
							Start = start.AddMonths(-2).AddDays(-1),
							End = start.AddMonths(2),
							Duration = PeriodDuration.Hours(2),
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(24/3*32, time.Length);
					for (int i = 0; i < 256; i++)
					{
						if (i == 40 ||i == 41 || i == 248)
							continue;
						Assert.Equal(0, time[i].Volume);
					}
					Assert.Equal("Time", time[40].DebugKey);
					Assert.Equal(PeriodDuration.Hours(3), time[40].Duration);
					Assert.Equal(1, time[40].Volume);
					Assert.Equal(6, time[40].Sum);
					Assert.Equal(6, time[40].Open);
					Assert.Equal(6, time[40].Close);
					Assert.Equal(6, time[40].High);
					Assert.Equal(6, time[40].Low);
					
					Assert.Equal(3, time[41].Volume);
					Assert.Equal("Time", time[41].DebugKey);
					Assert.Equal(PeriodDuration.Hours(3), time[41].Duration);
					Assert.Equal(3, time[41].Volume);
					Assert.Equal(36, time[41].Sum);
					Assert.Equal(9, time[41].Open);
					Assert.Equal(15, time[41].Close);
					Assert.Equal(15, time[41].High);
					Assert.Equal(9, time[41].Low);

					Assert.Equal("26.3333333333333", (time[248].Sum/time[248].Volume).ToString(CultureInfo.InvariantCulture));
					Assert.Equal("Time", time[248].DebugKey);
					Assert.Equal(PeriodDuration.Hours(3), time[248].Duration);
					Assert.Equal(3, time[248].Volume);
					Assert.Equal(79, time[248].Sum);
					Assert.Equal(10, time[248].Open);
					Assert.Equal(50, time[248].Close);
					Assert.Equal(50, time[248].High);
					Assert.Equal(10, time[248].Low);


					Assert.Equal((60 + 1 + 60) * 24 / 2, money.Length);
					for (int i = 0; i < 1452; i++)
					{
						if (i == 409 || i == 410 || i == 411 || i == 720 || i == 721)
							continue;
						
						Assert.Equal(0, money[i].Volume);
						Assert.Equal("Money", money[i].DebugKey);
					}
					Assert.Equal("Money", money[409].DebugKey);
					Assert.Equal(PeriodDuration.Hours(2), money[409].Duration);
					Assert.Equal(PeriodDuration.Hours(2), money[410].Duration);
					Assert.Equal(PeriodDuration.Hours(2), money[411].Duration);
					Assert.Equal(1, money[409].Volume);
					Assert.Equal(2, money[410].Volume);
					Assert.Equal(1, money[411].Volume);
					Assert.Equal(8, money[409].Sum);
					Assert.Equal(8, money[409].Open);
					Assert.Equal(8, money[409].Close);
					Assert.Equal(8, money[409].Low);
					Assert.Equal(8, money[409].High);
					Assert.Equal(25, money[410].Sum);
					Assert.Equal(11, money[410].Open);
					Assert.Equal(14, money[410].Close);
					Assert.Equal(11, money[410].Low);
					Assert.Equal(14, money[410].High);
					Assert.Equal(17, money[411].Sum);
					Assert.Equal(17, money[411].Open);
					Assert.Equal(17, money[411].Close);
					Assert.Equal(17, money[411].Low);
					Assert.Equal(17, money[411].High);

					Assert.Equal("Money", money[720].DebugKey);
					Assert.Equal(300, money[720].Sum / money[720].Volume);
					Assert.Equal(130, money[721].Sum / money[721].Volume);
					Assert.Equal(PeriodDuration.Hours(2), money[720].Duration);
					Assert.Equal(PeriodDuration.Hours(2), money[721].Duration);
					Assert.Equal(2, money[720].Volume);
					Assert.Equal(1, money[721].Volume);
					Assert.Equal(54, money[720].Open);
					Assert.Equal(130, money[721].Open);
					Assert.Equal(546, money[720].Close);
					Assert.Equal(130, money[721].Close);
					Assert.Equal(54, money[720].Low);
					Assert.Equal(130, money[721].Low);
					Assert.Equal(546, money[720].High);
					Assert.Equal(130, money[721].High);
				}
			}
		}

		[Fact]
		public void CanQueryDataInSpecificDuration_LowerDurationThanActualOnDisk()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.QueryRollup(
						new TimeSeriesRollupQuery
						{
							Key = "Time",
							Start = start.AddHours(-1),
							End = start.AddHours(4),
							Duration = PeriodDuration.Seconds(3),
						},
						new TimeSeriesRollupQuery
						{
							Key = "Money",
							Start = start.AddHours(-1),
							End = start.AddHours(4),
							Duration = PeriodDuration.Minutes(3),
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(5 * 60 * 60 / 3, time.Length);
					for (int i = 0; i < 6000; i++)
					{
						if (i == 1200 || i == 2400 || i == 3600)
							continue;
						Assert.Equal(PeriodDuration.Seconds(3), time[i].Duration);
						Assert.Equal("Time", time[i].DebugKey);
						Assert.Equal(0, time[i].Volume);
						Assert.Equal(0, time[i].Open);
						Assert.Equal(0, time[i].Close);
						Assert.Equal(0, time[i].Low);
						Assert.Equal(0, time[i].High);
						Assert.Equal(0, time[i].Sum);
						Assert.Equal(0, time[i].Volume);
					}
					Assert.Equal("Time", time[1200].DebugKey);
					Assert.Equal(PeriodDuration.Seconds(3), time[1200].Duration);
					Assert.Equal(PeriodDuration.Seconds(3), time[2400].Duration);
					Assert.Equal(PeriodDuration.Seconds(3), time[3600].Duration);
					Assert.Equal(1, time[1200].Volume);
					Assert.Equal(1, time[2400].Volume);
					Assert.Equal(1, time[3600].Volume);
					Assert.Equal(10, time[1200].Open);
					Assert.Equal(19, time[2400].Open);
					Assert.Equal(50, time[3600].Open);
					Assert.Equal(10, time[1200].Close);
					Assert.Equal(19, time[2400].Close);
					Assert.Equal(50, time[3600].Close);
					Assert.Equal(10, time[1200].Low);
					Assert.Equal(19, time[2400].Low);
					Assert.Equal(50, time[3600].Low);
					Assert.Equal(10, time[1200].High);
					Assert.Equal(19, time[2400].High);
					Assert.Equal(50, time[3600].High);
					Assert.Equal(10, time[1200].Sum);
					Assert.Equal(19, time[2400].Sum);
					Assert.Equal(50, time[3600].Sum);
					Assert.Equal(1, time[1200].Volume);
					Assert.Equal(1, time[2400].Volume);
					Assert.Equal(1, time[3600].Volume);


					Assert.Equal(5 * 60 / 3, money.Length);
					for (int i = 0; i < 100; i++)
					{
						if (i == 20 || i == 40 || i == 60)
							continue;
						Assert.Equal(PeriodDuration.Minutes(3), money[i].Duration);
						Assert.Equal("Money", money[i].DebugKey);
						Assert.Equal(0, money[i].Volume);
						Assert.Equal(0, money[i].Open);
						Assert.Equal(0, money[i].Close);
						Assert.Equal(0, money[i].Low);
						Assert.Equal(0, money[i].High);
						Assert.Equal(0, money[i].Sum);
						Assert.Equal(0, money[i].Volume);
					}
					Assert.Equal("Money", money[20].DebugKey);
					Assert.Equal(PeriodDuration.Minutes(3), money[20].Duration);
					Assert.Equal(PeriodDuration.Minutes(3), money[40].Duration);
					Assert.Equal(PeriodDuration.Minutes(3), money[60].Duration);
					Assert.Equal(1, money[20].Volume);
					Assert.Equal(1, money[40].Volume);
					Assert.Equal(1, money[60].Volume);
					Assert.Equal(54, money[20].Open);
					Assert.Equal(546, money[40].Open);
					Assert.Equal(130, money[60].Open);
					Assert.Equal(54, money[20].Close);
					Assert.Equal(546, money[40].Close);
					Assert.Equal(130, money[60].Close);
					Assert.Equal(54, money[20].Low);
					Assert.Equal(546, money[40].Low);
					Assert.Equal(130, money[60].Low);
					Assert.Equal(54, money[20].High);
					Assert.Equal(546, money[40].High);
					Assert.Equal(130, money[60].High);
					Assert.Equal(54, money[20].Sum);
					Assert.Equal(546, money[40].Sum);
					Assert.Equal(130, money[60].Sum);
					Assert.Equal(1, money[20].Volume);
					Assert.Equal(1, money[40].Volume);
					Assert.Equal(1, money[60].Volume);
				}
			}
		}

		[Fact]
		public void MissingDataInSeries()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTime(2015, 4, 1, 0, 0, 0);
				using (var r = tss.CreateReader())
				{
					var result = r.Query(
						new TimeSeriesQuery
						{
							Key = "Time",
							Start = start.AddSeconds(1),
							End = start.AddMinutes(30),
						},
						new TimeSeriesQuery
						{
							Key = "Money",
							Start = start.AddSeconds(1),
							End = start.AddMinutes(30),
						},
						new TimeSeriesQuery
						{
							Key = "Is",
							Start = start.AddSeconds(1),
							End = start.AddMinutes(30),
						}).ToArray();

					Assert.Equal(3, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();
					var Is = result[2].ToArray();

					Assert.Equal(0, time.Length);
					Assert.Equal(0, money.Length);
					Assert.Equal(0, Is.Length);
				}
			}
		}

		private static void WriteTestData(TimeSeriesStorage tss)
		{
			var start = new DateTime(2015, 4, 1, 0, 0, 0);
			var data = new[]
			{
				new {Key = "Time", At = start, Value = 10},
				new {Key = "Money", At = start, Value = 54},
				new {Key = "Is", At = start, Value = 1029},

				new {Key = "Money", At = start.AddHours(1), Value = 546},
				new {Key = "Is", At = start.AddHours(1), Value = 70},
				new {Key = "Time", At = start.AddHours(1), Value = 19},

				new {Key = "Is", At = start.AddHours(2), Value = 64},
				new {Key = "Money", At = start.AddHours(2), Value = 130},
				new {Key = "Time", At = start.AddHours(2), Value = 50},
			};

			var seriesType = SeriesType.Simple();
			var writer = tss.CreateWriter(seriesType);
			foreach (var item in data)
			{
				writer.Append(item.Key, item.At, item.Value);
			}

			writer.Commit();
			writer.Dispose();
		}
	}
}