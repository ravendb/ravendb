using System;

using Jint;
using Jint.Native;
using Jint.Native.Object;

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
			var p = new Person { Name = "Name1" };

			

			var engine = new Engine();

			var z = new ObjectInstance(engine) { Extensible = true };
			z.Put("Name", new JsValue("Name1"), false);

			engine.SetValue("p", z);
			engine.SetValue("Put", (Action<string, object, object>)((key, value, meta) => Put(key, value, meta, engine)));

			try
			{
				engine.Execute("Put('1');");
			}
			catch (Exception)
			{
			}
			
		}

		private static void Put(string key, object value, object meta, Engine engine)
		{
			throw new InvalidOperationException("Test message");
		}
	}
}
