using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.FileSystem;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.MailingList;

namespace Raven.Tryouts
{
	public class Program
	{
		private static void Main()
		{
           new FragmentFail().Fragment_Length_Should_Not_Be_More_Then_128();
		}

	}
}