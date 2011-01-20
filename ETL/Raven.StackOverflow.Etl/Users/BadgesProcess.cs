//-----------------------------------------------------------------------
// <copyright file="BadgesProcess.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Users
{
	public class BadgesProcess : EtlProcess
	{
		private readonly string path;

		public BadgesProcess(string path)
		{
			this.path = path;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SingleThreadedPipelineExecuter(); 
			Register(new XmlRowOperationFile(Path.Combine(path, "badges.xml")));
			Register(new AddBadgesToUser());
		}
	}
}
