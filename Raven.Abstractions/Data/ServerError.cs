//-----------------------------------------------------------------------
// <copyright file="ServerError.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	public class ServerError
	{
		public string Index { get; set; }
		public string Error { get; set; }
		public DateTime Timestamp { get; set; }
		public string Document { get; set; }

		public override string ToString()
		{
			return string.Format("Index: {0}, Error: {1}, Document: {2}", Index, Error, Document);
		}
	}
}