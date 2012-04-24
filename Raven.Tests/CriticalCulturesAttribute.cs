using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using Xunit.Extensions;

namespace Raven.Tests
{
	public class CriticalCulturesAttribute: DataAttribute
	{
		public override IEnumerable<object[]> GetData(MethodInfo methodUnderTest, Type[] parameterTypes)
		{
			var cultures = new[]
			{ 
				CultureInfo.InvariantCulture,
				CultureInfo.CurrentCulture,
				CultureInfo.GetCultureInfo("NL"), // Uses comma instead of point: 12,34
				CultureInfo.GetCultureInfo("tr-TR"), // "The Turkey Test"
			};
			return cultures.Select(c => new object[] { c });
		}
	}
}