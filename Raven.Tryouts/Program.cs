using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using Raven.Client.Document;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			var list = new HttpListener
			           	{
							AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication,
			           		Prefixes = {"http://+:8080/"},
			           		ExtendedProtectionSelectorDelegate = request =>
			           		                                     	{
			           		                                     		Console.WriteLine("request");
			           		                                     		return null;
			           		                                     	}
			           	};

			list.Start();

			while (true)
			{
				var r = list.GetContext();
				r.Response.Close();
			}
		}
	}
}