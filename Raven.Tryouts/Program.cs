using System;
using System.Diagnostics;
using System.Globalization;
using Raven.Client.Document;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main()
		{
			for (int i = 0; i < 100; i++)
			{
				Console.Clear();
				Console.WriteLine(i);
				using(var x = new AsyncCommit())
				{
					x.DtcCommitWillGiveNewResultIfNonAuthoritativeIsSetToFalse();
				}
			}
		} 
	}
}