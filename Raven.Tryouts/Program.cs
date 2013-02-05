using System;
using System.Globalization;

internal class Program
{
	private static void Main(string[] args)
	{
		var timeSpan = TimeSpan.FromMilliseconds(1283123);

		Console.WriteLine(ToLexicographicalString(timeSpan));
	}

	private static string ToLexicographicalString(TimeSpan timeSpan)
	{
		var remainingDays = (long) timeSpan.TotalDays;
		var totalDays = remainingDays;
		var sign = timeSpan.Ticks >= 0 ? "p" : "";
		var totalYears = totalDays/365;
		totalDays = totalDays%365;
		return string.Format("{0}-y{1:0,0000}-d{2:000}-{3}", sign, totalYears, totalDays, (timeSpan - TimeSpan.FromDays(remainingDays)).ToString());
	}

	private static float T(double p0)
	{
		return (float) (p0 + 180)*2;
	}
}

public class MyItem
{
	public string Id { get; set; }
	public string Hash { get; set; }
}