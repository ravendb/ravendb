using System;
using System.Globalization;
using System.Linq;
using Raven.Abstractions.TimeSeries;
using Raven.Database.TimeSeries;
using Voron;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesValueLengthTests : TimeSeriesTest
	{
		[Fact]
		public void CanQueryData_With3Values()
		{
			var start = new DateTime(2015, 4, 1, 0, 0, 0);

			using (var tss = GetStorage())
			{
				tss.CreateType(new TimeSeriesType {Type = "3Value", Fields = new[] {"Value", "Index", "Ticks"}});
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

				using (var writer = tss.CreateWriter())
				{
					foreach (var item in data)
					{
						writer.Append("3Value", item.Key, item.At, item.Value, StringToIndex(item.Key), item.At.Ticks);
					}
					writer.Commit();
				}

				using (var r = tss.CreateReader())
				{
					var result = r.Query(
						new TimeSeriesQuery
						{
							Type = "3Value",
							Key = "Time",
							Start = start.AddYears(-1),
							End = start.AddYears(1),
						},
						new TimeSeriesQuery
						{
							Type = "3Value",
							Key = "Money",
							Start = DateTime.MinValue,
							End = DateTime.MaxValue
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(3, time.Length);
					Assert.Equal(3, money.Length);

					for (int i = 0; i < 3; i++)
					{
#if DEBUG
						Assert.Equal("Time", time[i].DebugKey);
						Assert.Equal("Money", money[i].DebugKey);
#endif

						Assert.Equal(new DateTime(2015, 4, 1, i, 0, 0), time[i].At);
						Assert.Equal(time[i].At.Ticks, time[i].Values[2]);
						Assert.Equal(1, time[i].Values[1]);

						Assert.Equal(new DateTime(2015, 4, 1, i, 0, 0), money[i].At);
						Assert.Equal(money[i].At.Ticks, money[i].Values[2]);
						Assert.Equal(3, money[i].Values[1]);
					}
					Assert.Equal(10, time[0].Values[0]);
					Assert.Equal(19, time[1].Values[0]);
					Assert.Equal(50, time[2].Values[0]);
					Assert.Equal(54, money[0].Values[0]);
					Assert.Equal(546, money[1].Values[0]);
					Assert.Equal(130, money[2].Values[0]);
				}
			}
		}

		[Fact]
		public void CanQueryData_With3Values_Rollups()
		{
			var start = new DateTime(2015, 4, 1, 0, 0, 0);

			using (var tss = GetStorage())
			{
				tss.CreateType(new TimeSeriesType { Type = "3Val", Fields = new[] { "Value 1", "Value Two", "Value 3" } });
				using (var writer = tss.CreateWriter())
				{
					for (int i = 0; i < 7; i++)
					{
						writer.Append("3Val", "Money", start.AddHours(i), 1000 + i, StringToIndex("Money"), start.AddHours(i).Ticks);
						writer.Append("3Val", "Is", start.AddHours(i), 7000 + i, StringToIndex("Is"), start.AddHours(i).Ticks);
						writer.Append("3Val", "Time", start.AddHours(i), 19000 + i, StringToIndex("Time"), start.AddHours(i).Ticks);
					}
					writer.Commit();
				}

				using (var r = tss.CreateReader())
				{
					var result = r.QueryRollup(
						new TimeSeriesRollupQuery
						{
							Type = "3Val",
							Key = "Time",
							Start = start.AddMonths(-1),
							End = start.AddDays(1),
							Duration = PeriodDuration.Hours(3),
						},
						new TimeSeriesRollupQuery
						{
							Type = "3Val",
							Key = "Money",
							Start = start.AddDays(-1),
							End = start.AddMonths(1),
							Duration = PeriodDuration.Hours(2),
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(256, time.Length);
					for (int i = 0; i < 256; i++)
					{
#if DEBUG
						Assert.Equal("Time", time[i].DebugKey);
#endif
						Assert.Equal(start.AddMonths(-1).AddHours(i * 3), time[i].StartAt);

						if (i == 248 || i == 249 || i == 250)
						{
							Assert.Equal(i == 250 ? 1 : 3, time[i].Values[0].Volume);

							Assert.Equal(i == 250 ? 1 : 3, time[i].Values[1].Volume);
							Assert.Equal(i == 250 ? 1 : 3, time[i].Values[1].Sum);
							Assert.Equal(1, time[i].Values[1].Low);
							Assert.Equal(1, time[i].Values[1].Open);
							Assert.Equal(1, time[i].Values[1].Close);
							Assert.Equal(1, time[i].Values[1].High);

							Assert.Equal(i == 250 ? 1 : 3, time[i].Values[2].Volume);
							Assert.Equal(time[i].StartAt.Ticks, time[i].Values[2].Low);
							Assert.Equal(time[i].StartAt.Ticks, time[i].Values[2].Open);
							Assert.Equal(time[i].StartAt.AddHours(i == 250 ? 0 : 2).Ticks, time[i].Values[2].Close);
							Assert.Equal(time[i].StartAt.AddHours(i == 250 ? 0 : 2).Ticks, time[i].Values[2].High);
							if (i == 250)
								Assert.Equal(time[i].StartAt.Ticks, time[i].Values[2].Sum);
							else
								Assert.Equal(time[i].StartAt.Ticks + time[i].StartAt.AddHours(1).Ticks + time[i].StartAt.AddHours(2).Ticks, time[i].Values[2].Sum);

							continue;
						}

						for (int j = 0; j < 3; j++)
						{
							Assert.Equal(0, time[i].Values[j].Volume);
							Assert.Equal(0, time[i].Values[j].Sum);
							Assert.Equal(0, time[i].Values[j].Close);
							Assert.Equal(0, time[i].Values[j].High);
							Assert.Equal(0, time[i].Values[j].Low);
							Assert.Equal(0, time[i].Values[j].Open);
						}
					}
					Assert.Equal(19000, time[248].Values[0].Low);
					Assert.Equal(19000, time[248].Values[0].Open);
					Assert.Equal(19002, time[248].Values[0].Close);
					Assert.Equal(19002, time[248].Values[0].High);
					Assert.Equal(19000 * 3 + 3, time[248].Values[0].Sum);
					Assert.Equal(19003, time[249].Values[0].Low);
					Assert.Equal(19003, time[249].Values[0].Open);
					Assert.Equal(19005, time[249].Values[0].Close);
					Assert.Equal(19005, time[249].Values[0].High);
					Assert.Equal(19003 * 3 + 3, time[249].Values[0].Sum);
					Assert.Equal(19006, time[250].Values[0].Low);
					Assert.Equal(19006, time[250].Values[0].Open);
					Assert.Equal(19006, time[250].Values[0].Close);
					Assert.Equal(19006, time[250].Values[0].High);
					Assert.Equal(19006, time[250].Values[0].Sum);

					Assert.Equal(372, money.Length);
					for (int i = 0; i < 372; i++)
					{
#if DEBUG
						Assert.Equal("Money", money[i].DebugKey);
#endif
						Assert.Equal(start.AddDays(-1).AddHours(i * 2), money[i].StartAt);

						if (i >= 12 && i <= 16)
							continue;

						for (int j = 0; j < 3; j++)
						{
							Assert.Equal(0, money[i].Values[j].Volume);
							Assert.Equal(0, money[i].Values[j].Sum);
							Assert.Equal(0, money[i].Values[j].Close);
							Assert.Equal(0, money[i].Values[j].High);
							Assert.Equal(0, money[i].Values[j].Low);
							Assert.Equal(0, money[i].Values[j].Open);
						}
					}
				}


				var start2 = start.AddMonths(-1).AddDays(5);
				using (var writer = tss.CreateWriter())
				{
					int value = 6;
					for (int i = 0; i < 4; i++)
					{
						writer.Append("3Val", "Time", start2.AddHours(2 + i), value++, StringToIndex("Time"), start2.AddHours(2 + i).Ticks);
						writer.Append("3Val", "Is", start2.AddHours(2 + i), value++, StringToIndex("Is"), start2.AddHours(2 + i).Ticks);
						writer.Append("3Val", "Money", start2.AddHours(2 + i), value++, StringToIndex("Money"), start2.AddHours(2 + i).Ticks);
					}
					writer.Commit();
				}


				using (var r = tss.CreateReader())
				{
					var result = r.QueryRollup(
						new TimeSeriesRollupQuery
						{
							Type = "3Val",
							Key = "Time",
							Start = start.AddMonths(-1),
							End = start.AddDays(1),
							Duration = PeriodDuration.Hours(3),
						},
						new TimeSeriesRollupQuery
						{
							Type = "3Val",
							Key = "Money",
							Start = start.AddMonths(-2).AddDays(-1),
							End = start.AddMonths(2),
							Duration = PeriodDuration.Hours(2),
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(256, time.Length);
					for (int i = 0; i < 256; i++)
					{
#if DEBUG
						Assert.Equal("Time", time[i].DebugKey);
#endif
						Assert.Equal(start.AddMonths(-1).AddHours(i * 3), time[i].StartAt);

						if (i == 40 || i == 41)
						{
							Assert.Equal(i == 40 ? 1 : 3, time[i].Values[0].Volume);

							Assert.Equal(i == 40 ? 1 : 3, time[i].Values[1].Volume);
							Assert.Equal(i == 40 ? 1 : 3, time[i].Values[1].Sum);
							Assert.Equal(1, time[i].Values[1].Low);
							Assert.Equal(1, time[i].Values[1].Open);
							Assert.Equal(1, time[i].Values[1].Close);
							Assert.Equal(1, time[i].Values[1].High);

							Assert.Equal(i == 40 ? 1 : 3, time[i].Values[2].Volume);
							Assert.Equal(time[i].StartAt.AddHours(i == 40 ? 2 : 0).Ticks, time[i].Values[2].Low);
							Assert.Equal(time[i].StartAt.AddHours(i == 40 ? 2 : 0).Ticks, time[i].Values[2].Open);
							Assert.Equal(time[i].StartAt.AddHours(2).Ticks, time[i].Values[2].Close);
							Assert.Equal(time[i].StartAt.AddHours(2).Ticks, time[i].Values[2].High);
							if (i == 40)
								Assert.Equal(time[i].StartAt.AddHours(2).Ticks, time[i].Values[2].Sum);
							else
								Assert.Equal(time[i].StartAt.Ticks + time[i].StartAt.AddHours(1).Ticks + time[i].StartAt.AddHours(2).Ticks, time[i].Values[2].Sum);

							continue;
						}

						if (i == 248 || i == 249 || i == 250)
						{
							Assert.Equal(i == 250 ? 1 : 3, time[i].Values[0].Volume);

							Assert.Equal(i == 250 ? 1 : 3, time[i].Values[1].Volume);
							Assert.Equal(i == 250 ? 1 : 3, time[i].Values[1].Sum);
							Assert.Equal(1, time[i].Values[1].Low);
							Assert.Equal(1, time[i].Values[1].Open);
							Assert.Equal(1, time[i].Values[1].Close);
							Assert.Equal(1, time[i].Values[1].High);

							Assert.Equal(i == 250 ? 1 : 3, time[i].Values[2].Volume);
							Assert.Equal(time[i].StartAt.Ticks, time[i].Values[2].Low);
							Assert.Equal(time[i].StartAt.Ticks, time[i].Values[2].Open);
							Assert.Equal(time[i].StartAt.AddHours(i == 250 ? 0 : 2).Ticks, time[i].Values[2].Close);
							Assert.Equal(time[i].StartAt.AddHours(i == 250 ? 0 : 2).Ticks, time[i].Values[2].High);
							if (i == 250)
								Assert.Equal(time[i].StartAt.Ticks, time[i].Values[2].Sum);
							else
								Assert.Equal(time[i].StartAt.Ticks + time[i].StartAt.AddHours(1).Ticks + time[i].StartAt.AddHours(2).Ticks, time[i].Values[2].Sum);

							continue;
						}

						for (int j = 0; j < 3; j++)
						{
							Assert.Equal(0, time[i].Values[j].Volume);
							Assert.Equal(0, time[i].Values[j].Sum);
							Assert.Equal(0, time[i].Values[j].Close);
							Assert.Equal(0, time[i].Values[j].High);
							Assert.Equal(0, time[i].Values[j].Low);
							Assert.Equal(0, time[i].Values[j].Open);
						}
					}
					Assert.Equal(19000, time[248].Values[0].Low);
					Assert.Equal(19000, time[248].Values[0].Open);
					Assert.Equal(19002, time[248].Values[0].Close);
					Assert.Equal(19002, time[248].Values[0].High);
					Assert.Equal(19000 * 3 + 3, time[248].Values[0].Sum);
					Assert.Equal(19003, time[249].Values[0].Low);
					Assert.Equal(19003, time[249].Values[0].Open);
					Assert.Equal(19005, time[249].Values[0].Close);
					Assert.Equal(19005, time[249].Values[0].High);
					Assert.Equal(19003 * 3 + 3, time[249].Values[0].Sum);
					Assert.Equal(19006, time[250].Values[0].Low);
					Assert.Equal(19006, time[250].Values[0].Open);
					Assert.Equal(19006, time[250].Values[0].Close);
					Assert.Equal(19006, time[250].Values[0].High);
					Assert.Equal(19006, time[250].Values[0].Sum);

					Assert.Equal(1452, money.Length);
					for (int i = 0; i < 1452; i++)
					{
#if DEBUG
						Assert.Equal("Money", money[i].DebugKey);
#endif
						Assert.Equal(PeriodDuration.Hours(2), money[i].Duration);
						Assert.Equal(start.AddMonths(-2).AddDays(-1).AddHours(2 * i), money[i].StartAt);

						if ((i >= 409 && i <= 410) || 
							(i >= 720 && i <= 723))
						{
							Assert.Equal(i == 723 ? 1 : 2, money[i].Values[0].Volume);

							Assert.Equal(i == 723 ? 1 : 2, money[i].Values[1].Volume);
							Assert.Equal(i == 723 ? 3 : 6, money[i].Values[1].Sum);
							Assert.Equal(3, money[i].Values[1].Low);
							Assert.Equal(3, money[i].Values[1].Open);
							Assert.Equal(3, money[i].Values[1].Close);
							Assert.Equal(3, money[i].Values[1].High);

							Assert.Equal(i == 723 ? 1 : 2, money[i].Values[2].Volume);
							Assert.Equal(money[i].StartAt.Ticks, money[i].Values[2].Low);
							Assert.Equal(money[i].StartAt.Ticks, money[i].Values[2].Open);
							Assert.Equal(money[i].StartAt.AddHours(i == 723 ? 0 : 1).Ticks, money[i].Values[2].Close);
							Assert.Equal(money[i].StartAt.AddHours(i == 723 ? 0 : 1).Ticks, money[i].Values[2].High);
							if (i == 723)
								Assert.Equal(money[i].StartAt.Ticks, money[i].Values[2].Sum);
							else
								Assert.Equal(money[i].StartAt.Ticks + money[i].StartAt.AddHours(1).Ticks, money[i].Values[2].Sum);

							continue;
						}

						for (int j = 0; j < 3; j++)
						{
							Assert.Equal(0, money[i].Values[j].Volume);
							Assert.Equal(0, money[i].Values[j].Sum);
							Assert.Equal(0, money[i].Values[j].Close);
							Assert.Equal(0, money[i].Values[j].High);
							Assert.Equal(0, money[i].Values[j].Low);
							Assert.Equal(0, money[i].Values[j].Open);
						}
					}
				}
			}
		}

		private double StringToIndex(string key)
		{
			switch (key)
			{
				case "Time":
					return 1;
				case "Is":
					return 2;
				case "Money":
					return 3;
			}
			throw new ArgumentOutOfRangeException();
		}
	}
}