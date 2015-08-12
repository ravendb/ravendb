// -----------------------------------------------------------------------
//  <copyright file="PathInformation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics.Eventing.Reader;

namespace Raven.Monitor.IO.Data
{
	internal class PathInformation
	{
		public string Path { get; set; }

		public PathType PathType { get; set; }
	}
}