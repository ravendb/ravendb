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