using System;
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
			using (var x = new MultiTenant())
			{
				x.CanGetNotificationsFromTenant_DefaultDatabase();
			}
			using (var x = new MultiTenant())
			{
				x.CanGetNotificationsFromTenant_ExplicitDatabase();
			}
			using (var x = new MultiTenant())
			{
				x.CanGetNotificationsFromTenant_AndNotFromAnother();
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