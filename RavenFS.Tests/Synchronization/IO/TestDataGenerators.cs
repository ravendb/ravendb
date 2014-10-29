using System;
using System.IO;
using System.Text;

namespace RavenFS.Tests.Synchronization.IO
{
	public static class TestDataGenerators
	{
		public static void WriteNumbers(Stream output, int lines = 1000, Func<int, string, string> disturbator = null)
		{
			disturbator = disturbator ?? ((lineNumber, line) => line);
			var textWritter = new StreamWriter(output);
			var numberLength = Convert.ToInt32(Math.Ceiling(Math.Log10(lines)));
			for (var i = 0; i < lines; i++)
			{
				var stringBuilder = new StringBuilder();
				for (var j = 0; j < 100; j++)
				{
					stringBuilder.Append(i.ToString("D" + numberLength));
				}
				var line = disturbator(i, stringBuilder.ToString());
				textWritter.WriteLine(line);
			}
			textWritter.Flush();
		}
	}
}