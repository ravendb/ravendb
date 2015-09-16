// -----------------------------------------------------------------------
//  <copyright file="PrefixesInfoPageSection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Runtime.InteropServices;

namespace Voron.Trees
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct PrefixTreeInfoSection
	{
		[FieldOffset(0)]
		public byte NextPrefixId;

		[FieldOffset(1)]
		public unsafe fixed ushort PrefixOffsets[TreePage.PrefixCount];
	}
}