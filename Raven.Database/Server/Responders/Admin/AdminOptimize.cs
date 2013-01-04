// -----------------------------------------------------------------------
//  <copyright file="AdminOptimize.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminOptimize : AdminResponder
	{
		public override void RespondToAdmin(IHttpContext context)
		{
			Database.IndexStorage.MergeAllIndexes();
		}
	}
}