using System;
using Raven.Tests.Indexes;

internal class Program
{
	private static void Main(string[] args)
	{
		using (var x = new HighlightTesting())
		{
			x.HighlightText();
		}
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