// -----------------------------------------------------------------------
//  <copyright file="PrefixesTheory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Xml;
using Xunit.Extensions;
using Xunit.Sdk;

namespace Voron.Tests
{
	[CLSCompliant(false)]
	public class PrefixesTheoryAttribute : TheoryAttribute
	{
		protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
		{
			foreach (var command in base.EnumerateTestCommands(method))
			{
				yield return command;
			}

			using (PrefixesFactAttribute.TreesWithPrefixedKeys())
			{
				foreach (var command in base.EnumerateTestCommands(method))
				{
					yield return command;
				}
			}
		}
	}
}