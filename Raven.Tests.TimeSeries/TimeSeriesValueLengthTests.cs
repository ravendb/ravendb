using System;
using System.Globalization;
using System.Linq;
using Raven.Database.TimeSeries;
using Voron;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesValueLengthTests : TimeSeriesTest
	{
		[Fact]
		public void CanQueryData()
		{
			var start = new DateTime(2015, 4, 1, 0, 0, 0);

			using (var tss = GetStorage())
			{
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

				using (var writer = tss.CreateWriter(3))
				{
					foreach (var item in data)
					{
						writer.Append(item.Key, item.At, item.Value, StringToIndex(item.Key), item.At.Ticks);
					}
					writer.Commit();
				}

				using (var r = tss.CreateReader(3))
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
						}).ToArray();

					Assert.Equal(2, result.Length);
					var time = result[0].ToArray();
					var money = result[1].ToArray();

					Assert.Equal(3, time.Length);
					Assert.Equal(3, money.Length);

					for (int i = 0; i < 3; i++)
					{
						Assert.Equal("Time", time[i].DebugKey);
						Assert.Equal(new DateTime(2015, 4, 1, i, 0, 0), time[i].At);
						Assert.Equal(time[i].At.Ticks, time[i].Values[2]);
						Assert.Equal(1, time[i].Values[1]);

						Assert.Equal("Money", money[i].DebugKey);
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