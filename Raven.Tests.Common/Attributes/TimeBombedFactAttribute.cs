// -----------------------------------------------------------------------
//  <copyright file="TimeBombedFactAttribute.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Common.Attributes
{
	public class TimeBombedFactAttribute : FactAttribute
	{
		public TimeBombedFactAttribute(int year, int month, int day, string msg)
		{
			SkipUntil = new DateTime(year, month, day);
		}

		public DateTime SkipUntil { get; set; }

		protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
		{
			if (DateTime.Today < SkipUntil)
				return Enumerable.Empty<ITestCommand>();
			throw new InvalidOperationException("Time bombed fact expired");
			//return base.EnumerateTestCommands(method);
		}
	}
}