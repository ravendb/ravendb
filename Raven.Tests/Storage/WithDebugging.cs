//-----------------------------------------------------------------------
// <copyright file="WithDebugging.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using log4net.Appender;
using log4net.Config;
using log4net.Layout;

namespace Raven.Tests.Storage
{
	public class WithDebugging
	{
		static WithDebugging()
		{
			BasicConfigurator.Configure(
				new OutputDebugStringAppender
				{
					Layout = new SimpleLayout()
				});
		}
	}
}
