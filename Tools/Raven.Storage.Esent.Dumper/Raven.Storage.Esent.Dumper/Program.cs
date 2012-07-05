//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Isam.Esent.Utilities;

namespace Raven.Storage.Esent.Dumper
{
	class Program
	{
		static void Main(string[] args)
		{
			new Dbutil().DumpToCsv(args);
		}
	}
}
