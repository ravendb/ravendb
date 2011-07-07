// -----------------------------------------------------------------------
//  <copyright file="CanGetStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.ComponentModel;

namespace Raven.Studio.Infrastructure.Navigation
{
	public interface INavigatorMetdata
	{
		string Url { get; }

		[DefaultValue(int.MaxValue)]
		int Index { get; }
	}
}