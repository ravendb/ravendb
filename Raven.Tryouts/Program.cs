using System;
using System.Diagnostics;
using System.Linq;
using Raven.Database.Impl;
using Raven.Database.Util;
using Raven.Tests.Bugs;

namespace Raven.Tryouts
{
	internal class Program
	{
		[STAThread]
		private static void Main()
		{
			for (int i = 0; i < 1000; i++)
			{
				Console.WriteLine(i);
				using(var x = new AsyncCommit())
				{
					x.DtcCommitWillGiveNewResultIfNonAuthoritativeIsSetToFalseWhenQuerying();
				}
			}
		}
	}
}