using System;
using Raven.Tests.Issues;

class Program
{
	static void Main(string[] args)
	{
		for (int i = 0; i < 10; i++)
		{
			Console.WriteLine(i);
			using (var n = new RavenDB_1041())
			{
				n.CanSpecifyTimeoutWhenWaitingForReplication();
			}
		}
	}
}