// -----------------------------------------------------------------------
//  <copyright file="EnsureTestCleanup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Text;

using Xunit;

namespace Raven.Tests.Common.Attributes
{
	public class EnsureTestCleanupAttribute : BeforeAfterTestAttribute
	{
		private static string _lastTestName;
		public static void AssertPortsNotInUse(string test, params int[] ports)
		{
			IPGlobalProperties ipProperties = IPGlobalProperties.GetIPGlobalProperties();
			IPEndPoint[] ipEndPoints = ipProperties.GetActiveTcpListeners();

			var sb = new StringBuilder();

			foreach (IPEndPoint endPoint in ipEndPoints.Where(x => ports.Contains(x.Port)))
			{
				sb.AppendLine("Port " + endPoint.Port + " is in used but shouldn't be. Did we leak a connection in: " + test);
			}
			if (sb.Length > 0)
			{
				sb.AppendLine();
				Console.Error.WriteLine(sb.ToString());
				File.AppendAllText("invalid-test-output.txt", sb.ToString());
			}
		}

		public override void Before(MethodInfo methodUnderTest)
		{
			AssertPortsNotInUse(_lastTestName,8079, 8078, 8077, 8076, 8075);
		}

		public override void After(MethodInfo methodUnderTest)
		{
			_lastTestName = methodUnderTest.Name;
		}
	}
}