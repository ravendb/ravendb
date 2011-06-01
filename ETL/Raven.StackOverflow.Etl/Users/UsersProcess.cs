//-----------------------------------------------------------------------
// <copyright file="UsersProcess.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using ETL;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Users
{
	public class UsersProcess : EtlProcess
	{
		private readonly string _inputDirectory;
		private readonly string _outputDirectory;

		public UsersProcess(string inputDirectory, string outputDirectory)
		{
			_inputDirectory = inputDirectory;
			_outputDirectory = outputDirectory;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SimplePipelineExecutor();
			Register(new XmlRowOperationFile(Path.Combine(_inputDirectory, "users.xml")));
			Register(new RowToDatabase("Users", doc => "users/" + doc["Id"], _outputDirectory));
		}
	}
}
