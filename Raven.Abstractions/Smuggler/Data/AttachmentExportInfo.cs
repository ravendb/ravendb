// -----------------------------------------------------------------------
//  <copyright file="AttachmentExportInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler.Data
{
    [Obsolete("Use RavenFS instead.")]
	public class AttachmentExportInfo
	{
		public Stream Data { get; set; }
		public RavenJObject Metadata { get; set; }
		public string Key { get; set; }
	}
}