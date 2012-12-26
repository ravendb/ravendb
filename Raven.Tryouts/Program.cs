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
			for (int i = 0; i < 16; i++)
			{
				var bytes = new byte[16];
				bytes[i] = 1;
				Console.WriteLine(i + " " + new Guid(bytes));
			}
		}
	}
}