// -----------------------------------------------------------------------
//  <copyright file="TypeExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Voron.Util
{
	public static class TypeExtensions
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void AssertStructHasExplicitLayout(this Type structureType)
		{
			if (structureType.StructLayoutAttribute == null || 
				structureType.StructLayoutAttribute.Value == LayoutKind.Auto || structureType.StructLayoutAttribute.Pack != 1)
				throw new InvalidDataException("Specified type has to be a struct with StructLayout(LayoutKind.Explicit, Pack = 1) or StructLayout(LayoutKind.Sequential, Pack = 1) attribute applied");
		} 
	}
}