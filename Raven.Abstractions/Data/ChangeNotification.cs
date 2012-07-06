// -----------------------------------------------------------------------
//  <copyright file="ChangeNotification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	public class ChangeNotification : EventArgs
	{
		public ChangeType Type { get; set; }
		public string Name { get; set; }
		public Guid? Etag { get; set; }
	}

	public enum ChangeType
	{
		Put,
		Delete,
		IndexUpdated
	}
}