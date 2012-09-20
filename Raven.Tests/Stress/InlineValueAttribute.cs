using System;
using System.Collections.Generic;
using System.Reflection;
using Xunit.Extensions;

namespace Raven.Tests.Stress
{
	[CLSCompliant(false)]
	public class InlineValueAttribute : DataAttribute
	{
		readonly object[] val;

		public InlineValueAttribute(object x)
		{
			val = new[] { x };
		}

		public InlineValueAttribute(object x, object y, object z)
		{
			val = new[] {x, y, z};
		}

		public override IEnumerable<object[]> GetData(MethodInfo methodUnderTest, Type[] parameterTypes)
		{
			yield return val;
		}
	}
}