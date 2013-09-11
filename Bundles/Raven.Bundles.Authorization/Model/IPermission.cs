//-----------------------------------------------------------------------
// <copyright file="IPermission.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Bundles.Authorization.Model
{
	public interface IPermission
	{
		string Operation { get; set; }
		bool Allow { get; set; }
		int Priority { get; set; }

		string Explain { get; }
	}
}
