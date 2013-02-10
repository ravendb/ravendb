using System;
using Raven.Tests.Bugs;
using Raven.Tests.Indexes;
using Raven.Tests.Notifications;

internal class Program
{
	private static void Main(string[] args)
	{
		for (int i = 0; i < 100; i++)
		{
			Console.Clear();
			Console.WriteLine(i);
			using (var x = new AsyncCommit())
			{
				x.DtcCommitWillGiveNewResultIfNonAuthoritativeIsSetToFalseWhenQuerying();
			}
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