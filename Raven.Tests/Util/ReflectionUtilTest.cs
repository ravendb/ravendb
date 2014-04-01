//-----------------------------------------------------------------------
// <copyright file="ReflectionUtilTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Util
{
	public class ReflectionUtilTest : NoDisposalNeeded
	{
		[Fact]
		public void Can_generate_simple_type_name_for_simple_type()
		{
			var fullNameWithoutVersionInformation = ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(ReflectionUtilTest));
			
			Assert.Equal("Raven.Tests.Util.ReflectionUtilTest, Raven.Tests", fullNameWithoutVersionInformation);
			Assert.Equal(typeof(ReflectionUtilTest), Type.GetType(fullNameWithoutVersionInformation));
		}

		[Fact]
		public void Can_generate_simple_type_name_for_generic_type()
		{
			var fullNameWithoutVersionInformation = ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(List<ReflectionUtilTest>));

			Assert.Equal("System.Collections.Generic.List`1[[Raven.Tests.Util.ReflectionUtilTest, Raven.Tests]], mscorlib", fullNameWithoutVersionInformation);
			Assert.Equal(typeof(List<ReflectionUtilTest>), Type.GetType(fullNameWithoutVersionInformation));
		}
	}
}
