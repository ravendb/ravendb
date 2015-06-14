// -----------------------------------------------------------------------
//  <copyright file="FixedSizePage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.CodeDom;
using Voron.Impl;

namespace Voron.Trees.Fixed
{
	public unsafe class FixedSizePage
	{
		private readonly FixedSizeTree _owner;
		private readonly byte* _b;
		private FixedSizePageHeader* _header;

		public FixedSizePage(FixedSizeTree owner, byte* b)
		{
			_owner = owner;
			_b = b;

			_header = (FixedSizePageHeader*) b;
		}

		public byte* Data
		{
			get { return _b + Constants.PageHeaderSize; }
		}

		public FixedSizePageHeader* Header
		{
			get { return _header; }
		}
	}
}