using System;
using System.Collections.Generic;

namespace Raven.Tests.MonoForAndroid
{
	public static class MonoForAndroidTestBase
	{
		public static Dictionary<string, Action> Tests { get; set; }

		static MonoForAndroidTestBase()
		{
			Tests = new Dictionary<string, Action>();

			AddTest("Test1", () => { });
			AddTest("Test2", () => { });
		}

		public static void AddTest(string name, Action test)
		{
			Tests.Add(name, test);
		}
	}
}