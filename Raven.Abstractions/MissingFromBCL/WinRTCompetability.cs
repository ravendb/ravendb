// -----------------------------------------------------------------------
//  <copyright file="WinRTCompetability.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
#if !NETFX_CORE
using System;
using System.Collections.Generic;

namespace Raven.Abstractions.MissingFromBCL
{
	public static class WinRTCompetability
	{
		public static IEnumerable<char> ToCharArray(this string str)
		{
			return str;
		}

		public static Type AsType(this Type type)
		{
			return type;
		}
#if NETFX_CORE || SILVERLIGHT
		public static Type GetTypeInfo(this Type type)
		{
			return type;
		}
#endif
	}
}
#endif