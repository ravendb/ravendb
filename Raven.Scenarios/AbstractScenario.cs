//-----------------------------------------------------------------------
// <copyright file="AbstractScenario.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Xunit;

namespace Raven.Scenarios
{
	public abstract class AbstractScenario
	{
		[Fact]
		public void Execute()
		{
			new Scenario(
				Path.Combine(AllScenariosWithoutExplicitScenario.ScenariosPath, GetType().Name) + ".saz"
				).Execute();
		}
	}
}
