using System;

using Jint;

namespace Raven.Tryouts
{
	public class Program
	{
		public class Person
		{
			public string Name { get; set; }
		}

		public static void Main(string[] args)
		{
			var engine = new Engine();
			engine.SetValue("Put", (Action<string, object, object>)(Put));
			engine.Execute("Put('1', { });");
		}

		private static void Put(string key, object value, object meta)
		{
		}
	}
}
