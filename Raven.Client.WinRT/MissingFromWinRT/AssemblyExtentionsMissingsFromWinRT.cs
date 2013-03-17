// -----------------------------------------------------------------------
//  <copyright file="AssemblyExtentionsMissingsFromWinRT.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Reflection;
using Raven.Imports.Newtonsoft.Json.Utilities;

namespace Raven.Client.WinRT.MissingFromWinRT
{
	public static class AssemblyExtensionsMissingFromWinRt
	{
		 public static IEnumerable<TypeInfo> GetTypes(this Assembly assembly)
		 {
			 return assembly.DefinedTypes;
		 }

		 public static bool IsAssignableFrom(this Type type, TypeInfo typeInfo)
		 {
			 return type.IsAssignableFrom(typeInfo.AsType());
		 }
	}
}