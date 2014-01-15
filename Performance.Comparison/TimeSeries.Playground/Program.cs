using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
            var dts = new DateTimeSeries(@".\dts");
            using (var parser = new TextFieldParser(@"D:\TimeSeries.csv"))
            {
                parser.ReadLine();//ignore headers

                while (parser.EndOfData == false)
                {
                    var fields = parser.ReadFields();
                    Debug.Assert(fields != null);

                    dts.Add(fields[1], DateTime.ParseExact(fields[2],"Z",CultureInfo.InvariantCulture), double.Parse(fields[3]));
                }
            }
        }
    }
}
