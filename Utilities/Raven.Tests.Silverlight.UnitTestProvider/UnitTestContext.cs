// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.
namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using System;
	using System.Collections;
	using System.Collections.Generic;
	using System.Data;
	using System.Data.Common;
	using System.Globalization;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	public class UnitTestContext : TestContext
	{

		readonly TestMethod testMethod;

		Dictionary<string, string> propertyCache;

		internal UnitTestContext(TestMethod testMethod)
		{
			this.testMethod = testMethod;
		}

		public override IDictionary Properties
		{
			get
			{
				if (propertyCache == null)
				{
					propertyCache = new Dictionary<string, string>();
					foreach (var prop in testMethod.Properties)
					{
						propertyCache.Add(prop.Name, prop.Value);
					}
				}
				return propertyCache;
			}
		}

		public override DataRow DataRow
		{
			get { throw NotSupportedException("DataRow"); }
		}

		public override DbConnection DataConnection
		{
			get { throw NotSupportedException("DataConnection"); }
		}

		public override string TestName
		{
			get { return testMethod.Name; }
		}

		public override UnitTestOutcome CurrentTestOutcome
		{
			get { return UnitTestOutcome.Unknown; }
		}

		public override void WriteLine(string format, params object[] args)
		{
			var s = args.Length == 0 ? format : String.Format(CultureInfo.InvariantCulture, format, args);
			testMethod.OnWriteLine(s);
		}

		static Exception NotSupportedException(string functionality)
		{
			return
				new NotSupportedException(String.Format(CultureInfo.InvariantCulture, "UnitTestContext_FeatureNotSupported: {0}",
				                                        functionality));
		}

		public override void AddResultFile(string fileName)
		{
			throw NotSupportedException("AddResultFile");
		}

		public override void BeginTimer(string timerName)
		{
			throw NotSupportedException("BeginTimer");
		}

		public override void EndTimer(string timerName)
		{
			throw NotSupportedException("EndTimer");
		}
	}
}