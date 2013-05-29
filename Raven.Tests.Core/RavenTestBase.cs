// -----------------------------------------------------------------------
//  <copyright file="RavenTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Core
{
	public class RavenTestBase
	{
		protected string Url
		{
			get
			{
				if (UseFiddler)
					return "http://ipv4.fiddler:8079";
				return "http://localhost:8079";
			}
		}

		protected bool UseFiddler { get; set; }
	}
}