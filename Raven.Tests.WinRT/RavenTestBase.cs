// -----------------------------------------------------------------------
//  <copyright file="RavenTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.WinRT
{
	public class RavenTestBase
	{
		static RavenTestBase()
		{
			Url = "http://localhost:8079";
		}

		public RavenTestBase(bool useFiddler = false)
		{
			Url = useFiddler
				      ? "http://ipv4.fiddler:8079"
				      : "http://localhost:8079";
		}


		public static string Url { get; private set; }
	}
}