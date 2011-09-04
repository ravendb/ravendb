//-----------------------------------------------------------------------
// <copyright file="LocalClientTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Storage.Managed;
using Raven.Tests.Document;

namespace Raven.Tests
{
	public abstract class LocalClientTest : RavenTest 
	{
		
	}
}
