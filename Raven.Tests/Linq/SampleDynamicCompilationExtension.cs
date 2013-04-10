//-----------------------------------------------------------------------
// <copyright file="SampleDynamicCompilationExtension.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Database.Plugins;

namespace Raven.Tests.Linq
{
	public class SampleDynamicCompilationExtension : AbstractDynamicCompilationExtension
	{
		public override string[] GetNamespacesToImport()
		{
			return new[]
			{
				typeof (SampleGeoLocation).Namespace
			};
		}

		public override string[] GetAssembliesToReference()
		{
			return new[]
			{
				typeof (SampleGeoLocation).Assembly.Location
			};
		}
	}
}