//-----------------------------------------------------------------------
// <copyright file="RemoteManagedStorageState.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Munin
{
	[Serializable]
	public class RemoteManagedStorageState
	{
		public string Path { get; set; }
		public string Prefix { get; set; }

		public byte[] Log { get; set; }
	}
}