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
		private readonly string _inputDirectory;
		private readonly string _outputDirectory;

		public BadgesProcess(string inputDirectory, string outputDirectory)
		{
			_inputDirectory = inputDirectory;
			_outputDirectory = outputDirectory;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SingleThreadedPipelineExecuter(); 
			Register(new XmlRowOperationFile(Path.Combine(_inputDirectory, "badges.xml")));
			Register(new AddBadgesToUser(_outputDirectory));
		}
	}
}
