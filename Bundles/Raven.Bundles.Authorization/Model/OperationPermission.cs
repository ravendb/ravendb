//-----------------------------------------------------------------------
// <copyright file="OperationPermission.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Bundles.Authorization.Model
{
	public class OperationPermission : IPermission
	{
		public string Operation { get; set; }
		public List<string> Tags { get; set; }
		public bool Allow { get; set; }
		public int Priority { get; set; }

		public OperationPermission()
		{
			Tags = new List<string>();
		}

		[JsonIgnore]
		public string Explain
		{
			get
			{
				return string.Format("Operation: {0}, Tags: {1}, Allow: {2}, Priority: {3}", Operation, string.Join(", ", Tags ?? new List<string>()), Allow, Priority);
			}
		}
	}
}
