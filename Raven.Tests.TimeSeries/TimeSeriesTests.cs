using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Raven.Abstractions.TimeSeries;
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

				var start = new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero);
				using (var r = tss.CreateReader())
				{
					var time = r.GetPoints("Simple", "Time", start.AddYears(-1), start.AddYears(1)).ToArray();
					var money = r.GetPoints("Simple", "Money", DateTimeOffset.MinValue, DateTimeOffset.MaxValue).ToArray();

					Assert.Equal(3, time.Length);
					Assert.Equal(new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero), time[0].At);
					Assert.Equal(new DateTimeOffset(2015, 4, 1, 1, 0, 0, TimeSpan.Zero), time[1].At);
					Assert.Equal(new DateTimeOffset(2015, 4, 1, 2, 0, 0, TimeSpan.Zero), time[2].At);
					Assert.Equal(10, time[0].Values[0]);
					Assert.Equal(19, time[1].Values[0]);
					Assert.Equal(50, time[2].Values[0]);
#if DEBUG
					Assert.Equal("Time", time[0].DebugKey);
					Assert.Equal("Time", time[1].DebugKey);
					Assert.Equal("Time", time[2].DebugKey);
#endif
					
					Assert.Equal(3, money.Length);
#if DEBUG
					Assert.Equal("Money", money[0].DebugKey);
					Assert.Equal("Money", money[1].DebugKey);
					Assert.Equal("Money", money[2].DebugKey);
#endif
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
					var money = r.GetPoints("Simple", "Money", DateTimeOffset.MinValue, DateTimeOffset.MaxValue).ToArray();
					Assert.Equal(3, money.Length);
#if DEBUG
					Assert.Equal("Money", money[0].DebugKey);
					Assert.Equal("Money", money[1].DebugKey);
					Assert.Equal("Money", money[2].DebugKey);
#endif
				}
			}
		}

		[Fact]
		public void CanQueryDataInSpecificDurationSum()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero);
				using (var r = tss.CreateReader())
				{
					var time = r.GetAggregatedPoints("Simple", "Time", AggregationDuration.Hours(6), start.AddDays(-1), start.AddDays(2)).ToArray();
					var money = r.GetAggregatedPoints("Simple", "Money", AggregationDuration.Hours(2), start.AddDays(-2), start.AddDays(1)).ToArray();

					Assert.Equal(12, time.Length);
					Assert.Equal(new DateTimeOffset(2015, 3, 31, 0, 0, 0, TimeSpan.Zero), time[0].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 3, 31, 6, 0, 0, TimeSpan.Zero), time[1].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 3, 31, 12, 0, 0, TimeSpan.Zero), time[2].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 3, 31, 18, 0, 0, TimeSpan.Zero), time[3].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero), time[4].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 4, 1, 6, 0, 0, TimeSpan.Zero), time[5].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 4, 1, 12, 0, 0, TimeSpan.Zero), time[6].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 4, 1, 18, 0, 0, TimeSpan.Zero), time[7].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 4, 2, 0, 0, 0, TimeSpan.Zero), time[8].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 4, 2, 6, 0, 0, TimeSpan.Zero), time[9].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 4, 2, 12, 0, 0, TimeSpan.Zero), time[10].StartAt);
					Assert.Equal(new DateTimeOffset(2015, 4, 2, 18, 0, 0, TimeSpan.Zero), time[11].StartAt);
					Assert.Equal(79, time[4].Values[0].Sum);
#if DEBUG
					Assert.Equal("Time", time[4].DebugKey);
#endif
					Assert.Equal(AggregationDuration.Hours(6), time[4].Duration);

					Assert.Equal(36, money.Length);
					for (int i = 0; i < 36; i++)
					{
#if DEBUG
						Assert.Equal("Money", money[i].DebugKey);
#endif
						Assert.Equal(AggregationDuration.Hours(2), money[0].Duration);
						if (i == 24 || i == 25)
							continue;
						Assert.Equal(0, money[i].Values[0].Sum);
						Assert.Equal(0, money[i].Values[0].Volume);
						Assert.Equal(0, money[i].Values[0].High);
					}
					Assert.Equal(600, money[24].Values[0].Sum);
					Assert.Equal(2, money[24].Values[0].Volume);
					Assert.Equal(130, money[25].Values[0].Sum);
					Assert.Equal(1, money[25].Values[0].Volume);
				}
			}
		}

		[Fact]
		public void CanQueryDataInSpecificDurationAverage()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero);
				using (var r = tss.CreateReader())
				{
					var time = r.GetAggregatedPoints("Simple", "Time", AggregationDuration.Hours(3), start.AddMonths(-1), start.AddDays(1)).ToArray();
					var money = r.GetAggregatedPoints("Simple", "Money", AggregationDuration.Hours(2), start.AddDays(-1), start.AddMonths(1)).ToArray();

					Assert.Equal(24 / 3 * 32, time.Length);
					for (int i = 0; i < 256; i++)
					{
#if DEBUG
						Assert.Equal("Time", time[i].DebugKey);
#endif
						Assert.Equal(AggregationDuration.Hours(3), time[i].Duration);
						if (i == 248 || i == 249)
							continue;
						Assert.Equal(0, time[i].Values[0].Volume);
					}
					Assert.Equal("26.3333333333333", (time[248].Values[0].Sum / time[248].Values[0].Volume).ToString(CultureInfo.InvariantCulture));
					Assert.Equal(3, time[248].Values[0].Volume);
					Assert.Equal(10, time[248].Values[0].Open);
					Assert.Equal(50, time[248].Values[0].Close);
					Assert.Equal(50, time[248].Values[0].High);
					Assert.Equal(10, time[248].Values[0].Low);


					Assert.Equal(24 / 2 * 31, money.Length);
					for (int i = 0; i < 372; i++)
					{
						if (i == 12 || i == 13)
							continue;
						Assert.Equal(0, money[i].Values[0].Volume);
					}
#if DEBUG
					Assert.Equal("Money", money[12].DebugKey);
#endif
					Assert.Equal(300, money[12].Values[0].Sum / money[12].Values[0].Volume);
					Assert.Equal(130, money[13].Values[0].Sum / money[13].Values[0].Volume);
					Assert.Equal(AggregationDuration.Hours(2), money[12].Duration);
					Assert.Equal(AggregationDuration.Hours(2), money[13].Duration);
					Assert.Equal(2, money[12].Values[0].Volume);
					Assert.Equal(1, money[13].Values[0].Volume);
					Assert.Equal(54, money[12].Values[0].Open);
					Assert.Equal(130, money[13].Values[0].Open);
					Assert.Equal(546, money[12].Values[0].Close);
					Assert.Equal(130, money[13].Values[0].Close);
					Assert.Equal(54, money[12].Values[0].Low);
					Assert.Equal(130, money[13].Values[0].Low);
					Assert.Equal(546, money[12].Values[0].High);
					Assert.Equal(130, money[13].Values[0].High);
				}
			}
		}

		[Fact]
		public void CanQueryDataInSpecificDurationAverageUpdateRollup()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero);
				using (var r = tss.CreateReader())
				{
					var time = r.GetAggregatedPoints("Simple", "Time", AggregationDuration.Hours(3), start.AddMonths(-1), start.AddDays(1)).ToArray();
					var money = r.GetAggregatedPoints("Simple", "Money", AggregationDuration.Hours(2), start.AddDays(-1), start.AddMonths(1)).ToArray();

					Assert.Equal(24/3*32, time.Length);
					for (int i = 0; i < 256; i++)
					{
						if (i == 248)
							continue;
						Assert.Equal(0, time[i].Values[0].Volume);
					}
					Assert.Equal("26.3333333333333", (time[248].Values[0].Sum / time[248].Values[0].Volume).ToString(CultureInfo.InvariantCulture));
#if DEBUG
					Assert.Equal("Time", time[248].DebugKey);
#endif
					Assert.Equal(AggregationDuration.Hours(3), time[248].Duration);
					Assert.Equal(3, time[248].Values[0].Volume);
					Assert.Equal(10, time[248].Values[0].Open);
					Assert.Equal(50, time[248].Values[0].Close);
					Assert.Equal(50, time[248].Values[0].High);
					Assert.Equal(10, time[248].Values[0].Low);

					Assert.Equal(24/2*31, money.Length);
					for (int i = 0; i < 372; i++)
					{
						if (i == 12 || i == 13)
							continue;
						Assert.Equal(0, money[i].Values[0].Volume);
					}
#if DEBUG
					Assert.Equal("Money", money[12].DebugKey);
#endif
					Assert.Equal(300, money[12].Values[0].Sum / money[12].Values[0].Volume);
					Assert.Equal(130, money[13].Values[0].Sum / money[13].Values[0].Volume);
					Assert.Equal(AggregationDuration.Hours(2), money[12].Duration);
					Assert.Equal(AggregationDuration.Hours(2), money[13].Duration);
					Assert.Equal(2, money[12].Values[0].Volume);
					Assert.Equal(1, money[13].Values[0].Volume);
					Assert.Equal(54, money[12].Values[0].Open);
					Assert.Equal(130, money[13].Values[0].Open);
					Assert.Equal(546, money[12].Values[0].Close);
					Assert.Equal(130, money[13].Values[0].Close);
					Assert.Equal(54, money[12].Values[0].Low);
					Assert.Equal(130, money[13].Values[0].Low);
					Assert.Equal(546, money[12].Values[0].High);
					Assert.Equal(130, money[13].Values[0].High);
				}

				var start2 = start.AddMonths(-1).AddDays(5);
				using (var writer = tss.CreateWriter())
				{
					int value = 6;
					for (int i = 0; i < 4; i++)
					{
						writer.Append("Simple", "Time", start2.AddHours(2 + i), value++);
						writer.Append("Simple", "Is", start2.AddHours(3 + i), value++);
						writer.Append("Simple", "Money", start2.AddHours(3 + i), value++);
					}
					writer.Commit();
				}

				using (var r = tss.CreateReader())
				{
					var time = r.GetAggregatedPoints("Simple", "Time", AggregationDuration.Hours(3), start.AddMonths(-1), start.AddDays(1)).ToArray();
					var money = r.GetAggregatedPoints("Simple", "Money", AggregationDuration.Hours(2), start.AddMonths(-2).AddDays(-1), start.AddMonths(2)).ToArray();

					Assert.Equal(24/3*32, time.Length);
					for (int i = 0; i < 256; i++)
					{
#if DEBUG
						Assert.Equal("Time", time[i].DebugKey);
#endif
						Assert.Equal(AggregationDuration.Hours(3), time[i].Duration);
						if (i == 40 ||i == 41 || i == 248)
							continue;
						Assert.Equal(0, time[i].Values[0].Volume);
					}
					
					Assert.Equal(1, time[40].Values[0].Volume);
					Assert.Equal(6, time[40].Values[0].Sum);
					Assert.Equal(6, time[40].Values[0].Open);
					Assert.Equal(6, time[40].Values[0].Close);
					Assert.Equal(6, time[40].Values[0].High);
					Assert.Equal(6, time[40].Values[0].Low);

					Assert.Equal(3, time[41].Values[0].Volume);
					Assert.Equal(36, time[41].Values[0].Sum);
					Assert.Equal(9, time[41].Values[0].Open);
					Assert.Equal(15, time[41].Values[0].Close);
					Assert.Equal(15, time[41].Values[0].High);
					Assert.Equal(9, time[41].Values[0].Low);

					Assert.Equal("26.3333333333333", (time[248].Values[0].Sum / time[248].Values[0].Volume).ToString(CultureInfo.InvariantCulture));
					Assert.Equal(3, time[248].Values[0].Volume);
					Assert.Equal(79, time[248].Values[0].Sum);
					Assert.Equal(10, time[248].Values[0].Open);
					Assert.Equal(50, time[248].Values[0].Close);
					Assert.Equal(50, time[248].Values[0].High);
					Assert.Equal(10, time[248].Values[0].Low);


					Assert.Equal((60 + 1 + 60) * 24 / 2, money.Length);
					for (int i = 0; i < 1452; i++)
					{
#if DEBUG
						Assert.Equal("Money", money[i].DebugKey);
#endif
						Assert.Equal(AggregationDuration.Hours(2), money[i].Duration);
						if (i == 409 || i == 410 || i == 411 || i == 720 || i == 721)
							continue;
						
						Assert.Equal(0, money[i].Values[0].Volume);
					}
					Assert.Equal(1, money[409].Values[0].Volume);
					Assert.Equal(2, money[410].Values[0].Volume);
					Assert.Equal(1, money[411].Values[0].Volume);
					Assert.Equal(8, money[409].Values[0].Sum);
					Assert.Equal(8, money[409].Values[0].Open);
					Assert.Equal(8, money[409].Values[0].Close);
					Assert.Equal(8, money[409].Values[0].Low);
					Assert.Equal(8, money[409].Values[0].High);
					Assert.Equal(25, money[410].Values[0].Sum);
					Assert.Equal(11, money[410].Values[0].Open);
					Assert.Equal(14, money[410].Values[0].Close);
					Assert.Equal(11, money[410].Values[0].Low);
					Assert.Equal(14, money[410].Values[0].High);
					Assert.Equal(17, money[411].Values[0].Sum);
					Assert.Equal(17, money[411].Values[0].Open);
					Assert.Equal(17, money[411].Values[0].Close);
					Assert.Equal(17, money[411].Values[0].Low);
					Assert.Equal(17, money[411].Values[0].High);

					Assert.Equal(300, money[720].Values[0].Sum / money[720].Values[0].Volume);
					Assert.Equal(130, money[721].Values[0].Sum / money[721].Values[0].Volume);
					Assert.Equal(2, money[720].Values[0].Volume);
					Assert.Equal(1, money[721].Values[0].Volume);
					Assert.Equal(54, money[720].Values[0].Open);
					Assert.Equal(130, money[721].Values[0].Open);
					Assert.Equal(546, money[720].Values[0].Close);
					Assert.Equal(130, money[721].Values[0].Close);
					Assert.Equal(54, money[720].Values[0].Low);
					Assert.Equal(130, money[721].Values[0].Low);
					Assert.Equal(546, money[720].Values[0].High);
					Assert.Equal(130, money[721].Values[0].High);
				}
			}
		}

		[Fact]
		public void CanQueryDataInSpecificDuration_LowerDurationThanActualOnDisk()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero);
				using (var r = tss.CreateReader())
				{
					var time = r.GetAggregatedPoints("Simple", "Time", AggregationDuration.Seconds(3), start.AddHours(-1), start.AddHours(4)).ToArray();
					var money = r.GetAggregatedPoints("Simple", "Money", AggregationDuration.Minutes(3), start.AddHours(-1), start.AddHours(4)).ToArray();

					Assert.Equal(5 * 60 * 60 / 3, time.Length);
					for (int i = 0; i < 6000; i++)
					{
						if (i == 1200 || i == 2400 || i == 3600)
							continue;
						Assert.Equal(AggregationDuration.Seconds(3), time[i].Duration);
#if DEBUG
						Assert.Equal("Time", time[i].DebugKey);
#endif
						Assert.Equal(0, time[i].Values[0].Volume);
						Assert.Equal(0, time[i].Values[0].Open);
						Assert.Equal(0, time[i].Values[0].Close);
						Assert.Equal(0, time[i].Values[0].Low);
						Assert.Equal(0, time[i].Values[0].High);
						Assert.Equal(0, time[i].Values[0].Sum);
						Assert.Equal(0, time[i].Values[0].Volume);
					}
#if DEBUG
					Assert.Equal("Time", time[1200].DebugKey);
#endif
					Assert.Equal(AggregationDuration.Seconds(3), time[1200].Duration);
					Assert.Equal(AggregationDuration.Seconds(3), time[2400].Duration);
					Assert.Equal(AggregationDuration.Seconds(3), time[3600].Duration);
					Assert.Equal(1, time[1200].Values[0].Volume);
					Assert.Equal(1, time[2400].Values[0].Volume);
					Assert.Equal(1, time[3600].Values[0].Volume);
					Assert.Equal(10, time[1200].Values[0].Open);
					Assert.Equal(19, time[2400].Values[0].Open);
					Assert.Equal(50, time[3600].Values[0].Open);
					Assert.Equal(10, time[1200].Values[0].Close);
					Assert.Equal(19, time[2400].Values[0].Close);
					Assert.Equal(50, time[3600].Values[0].Close);
					Assert.Equal(10, time[1200].Values[0].Low);
					Assert.Equal(19, time[2400].Values[0].Low);
					Assert.Equal(50, time[3600].Values[0].Low);
					Assert.Equal(10, time[1200].Values[0].High);
					Assert.Equal(19, time[2400].Values[0].High);
					Assert.Equal(50, time[3600].Values[0].High);
					Assert.Equal(10, time[1200].Values[0].Sum);
					Assert.Equal(19, time[2400].Values[0].Sum);
					Assert.Equal(50, time[3600].Values[0].Sum);
					Assert.Equal(1, time[1200].Values[0].Volume);
					Assert.Equal(1, time[2400].Values[0].Volume);
					Assert.Equal(1, time[3600].Values[0].Volume);


					Assert.Equal(5 * 60 / 3, money.Length);
					for (int i = 0; i < 100; i++)
					{
#if DEBUG
						Assert.Equal("Money", money[i].DebugKey);
#endif
						Assert.Equal(AggregationDuration.Minutes(3), money[i].Duration);
						if (i == 20 || i == 40 || i == 60)
							continue;

						Assert.Equal(0, money[i].Values[0].Volume);
						Assert.Equal(0, money[i].Values[0].Open);
						Assert.Equal(0, money[i].Values[0].Close);
						Assert.Equal(0, money[i].Values[0].Low);
						Assert.Equal(0, money[i].Values[0].High);
						Assert.Equal(0, money[i].Values[0].Sum);
						Assert.Equal(0, money[i].Values[0].Volume);
					}
					Assert.Equal(1, money[20].Values[0].Volume);
					Assert.Equal(1, money[40].Values[0].Volume);
					Assert.Equal(1, money[60].Values[0].Volume);
					Assert.Equal(54, money[20].Values[0].Open);
					Assert.Equal(546, money[40].Values[0].Open);
					Assert.Equal(130, money[60].Values[0].Open);
					Assert.Equal(54, money[20].Values[0].Close);
					Assert.Equal(546, money[40].Values[0].Close);
					Assert.Equal(130, money[60].Values[0].Close);
					Assert.Equal(54, money[20].Values[0].Low);
					Assert.Equal(546, money[40].Values[0].Low);
					Assert.Equal(130, money[60].Values[0].Low);
					Assert.Equal(54, money[20].Values[0].High);
					Assert.Equal(546, money[40].Values[0].High);
					Assert.Equal(130, money[60].Values[0].High);
					Assert.Equal(54, money[20].Values[0].Sum);
					Assert.Equal(546, money[40].Values[0].Sum);
					Assert.Equal(130, money[60].Values[0].Sum);
					Assert.Equal(1, money[20].Values[0].Volume);
					Assert.Equal(1, money[40].Values[0].Volume);
					Assert.Equal(1, money[60].Values[0].Volume);
				}
			}
		}

		[Fact]
		public void MissingDataInSeries()
		{
			using (var tss = GetStorage())
			{
				WriteTestData(tss);

				var start = new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero);
				using (var r = tss.CreateReader())
				{
					var time = r.GetPoints("Simple", "Time", start.AddSeconds(1), start.AddMinutes(30)).ToArray();
					var money = r.GetPoints("Simple", "Money", start.AddSeconds(1), start.AddMinutes(30)).ToArray();
					var Is = r.GetPoints("Simple", "Is", start.AddSeconds(1), start.AddMinutes(30)).ToArray();

					Assert.Equal(0, time.Length);
					Assert.Equal(0, money.Length);
					Assert.Equal(0, Is.Length);
				}
			}
		}

		private static void WriteTestData(TimeSeriesStorage tss)
		{
			var start = new DateTimeOffset(2015, 4, 1, 0, 0, 0, TimeSpan.Zero);
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

			var writer = tss.CreateWriter();
			foreach (var item in data)
			{
				writer.Append("Simple", item.Key, item.At, item.Value);
			}

			writer.Commit();
			writer.Dispose();
		}
	}
}