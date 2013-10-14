// -----------------------------------------------------------------------
//  <copyright file="StreamResult.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
	public class StreamResult<T>
	{
		 public string Key { get; set; }
		 public Etag Etag { get; set; }
		 public RavenJObject Metadata { get; set; }
		 public T Document { get; set; }
	}
}