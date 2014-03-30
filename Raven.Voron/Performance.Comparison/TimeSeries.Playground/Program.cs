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
            //    Directory.Delete(@".\dts", true);
            var dts = new DateTimeSeries(@".\dts");
            foreach (var id in dts.ScanIds())
            {
                Console.WriteLine(id);
            }
            for (int i = 0; i < 10; i++)
            {
                var sp = Stopwatch.StartNew();

                Console.WriteLine(dts.ScanRanges(DateTime.MinValue, DateTime.MaxValue, new[]
                {
                    "6febe146-e893-4f64-89f8-527f2dbaae9b",
                    "707dcb42-c551-4f1a-9203-e4b0852516cf",
                    "74d5bee8-9a7b-4d4e-bd85-5f92dfc22edb",
                    "7ae29feb-6178-4930-bc38-a90adf99cfd3",
                }).SelectMany(x => x.Values).Count(x => x != null));

                Console.WriteLine(sp.Elapsed);
            }
            //int i = 0;
            //using (var parser = new TextFieldParser(@"D:\TimeSeries.csv"))
            //{
            //    parser.HasFieldsEnclosedInQuotes = true;
            //    parser.Delimiters = new[] { "," };
            //    parser.ReadLine();//ignore headers
            //    var startNew = Stopwatch.StartNew();
            //    while (parser.EndOfData == false)
            //    {
            //        var fields = parser.ReadFields();
            //        Debug.Assert(fields != null);

            //        dts.Add(fields[1], DateTime.ParseExact(fields[2], "o", CultureInfo.InvariantCulture), double.Parse(fields[3]));
            //        i++;
            //        if (i % 10000 == 0)
            //            Console.Write("\r{0,15:#,#}          ", i);
            //    }
            //    dts.Flush();
            //    Console.Write("\r{0,15:#,#}          ", i);
            //    Console.WriteLine(startNew.Elapsed);
            //}
        }
    }
}
