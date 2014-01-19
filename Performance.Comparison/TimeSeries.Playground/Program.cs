using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic.FileIO;

namespace TimeSeries.Playground
{
    class Program
    {
        static void Main(string[] args)
        {
			//if (Directory.Exists(@".\dts"))
			//	Directory.Delete(@".\dts", true);
            var dts = new DateTimeSeries(@".\dts");
	        foreach (var id in dts.ScanIds())
	        {
		        Console.WriteLine(id);
	        }
	        for (int i = 0; i < 10; i++)
	        {
				var sp = Stopwatch.StartNew();

				Console.WriteLine(dts.ScanRange("6518249a-9d0d-4e0a-9c08-0802f5c8c3d8", DateTime.MinValue, DateTime.MaxValue).Count());

				Console.WriteLine(sp.Elapsed);
	        }
			//int i = 0;
			//using (var parser = new TextFieldParser(@"C:\Users\Ayende\Downloads\TimeSeries.csv"))
			//{
			//	parser.HasFieldsEnclosedInQuotes = true;
			//	parser.Delimiters = new[] { "," };
			//	parser.ReadLine();//ignore headers
			//	var startNew = Stopwatch.StartNew();
			//	while (parser.EndOfData == false)
			//	{
			//		var fields = parser.ReadFields();
			//		Debug.Assert(fields != null);

			//		dts.Add(fields[1], DateTime.ParseExact(fields[2], "o", CultureInfo.InvariantCulture), double.Parse(fields[3]));
			//		i++;
			//		if (i % 10000 == 0)
			//			Console.Write("\r{0,15:#,#}          ", i);
			//	}
			//	dts.Flush();
			//	Console.Write("\r{0,15:#,#}          ", i);
			//	Console.WriteLine(startNew.Elapsed);
			//}
        }
    }
}
