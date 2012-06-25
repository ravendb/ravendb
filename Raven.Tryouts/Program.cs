using System;
using System.Diagnostics;
using System.Globalization;
using Raven.Client.Document;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			var val = "2012-05-31T20:58:43.9785585Z";
			var formats = new[] { "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff", "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffffK" };

			try
			{
				var dateTime = DateTime.ParseExact(val, formats, CultureInfo.InvariantCulture, DateTimeStyles.None);
				Console.WriteLine(dateTime.Kind);
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		} 
	}
}