//-----------------------------------------------------------------------
// <copyright file="AllScenariosWithoutExplicitScenario.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Xunit.Extensions;

namespace Raven.Scenarios
{
	public class AllScenariosWithoutExplicitScenario
	{
		public static string ScenariosPath
		{
			get
			{
				return Directory.Exists(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\debug")) // running in VS
					? Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\Scenarios")
					: Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\Raven.Scenarios\Scenarios");
			}
		}

		public static IEnumerable<object[]> ScenariosWithoutExplicitScenario
		{
			get
			{
				foreach (var file in Directory.GetFiles(ScenariosPath, "*.saz"))
				{
					if (
						typeof (Scenario).Assembly.GetType("Raven.Scenarios." + Path.GetFileNameWithoutExtension(file) +
							"Scenario") != null)
						continue;
					yield return new object[] {Path.GetFileNameWithoutExtension(file)};
				}
				;
			}
		}

		[Theory]
		[PropertyData("ScenariosWithoutExplicitScenario")]
		public void Execute(string file)
		{
			new Scenario(Path.Combine(ScenariosPath, file + ".saz")).Execute();
		}
	}
}
