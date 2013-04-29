//-----------------------------------------------------------------------
// <copyright file="CommitInfo.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Tests.Linq
{
	public class CommitInfo
	{
		public string Id { get; set; }
		public string Author { get; set; }
		public string PathInRepo { get; set; }
		public string Repository { get; set; }
		public int Revision { get; set; }
		public DateTime Date { get; set; }
		public string CommitMessage { get; set; }
	}
}