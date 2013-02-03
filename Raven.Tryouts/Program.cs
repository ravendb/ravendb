using System;

internal class Program
{
	private static void Main(string[] args)
	{
		Console.WriteLine(TimeSpan.FromHours(1123123).ToString(@"dddddddd\.hh\:mm\:ss\.fffffff"));
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