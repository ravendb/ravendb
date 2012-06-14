//-----------------------------------------------------------------------
// <copyright file="DocumentPermission.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Bundles.Authorization.Model
{
	public class DocumentPermission : IPermission
	{
		public string Operation { get; set; }
		public string User { get; set; }
		public string Role { get; set; }
		public bool Allow { get; set; }
		public int Priority { get; set; }

		[JsonIgnore]
		public string Explain
		{
			get
			{
				return string.Format("Operation: {0}, User: {1}, Role: {2}, Allow: {3}, Priority: {4}", Operation, User, Role, Allow,
				                     Priority);
			}
		}
	}
}
