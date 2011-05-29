//-----------------------------------------------------------------------
// <copyright file="VotesProcess.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using ETL;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Posts
{
	public class VotesProcess : EtlProcess
	{
		private readonly string _inputPath;
		private readonly string _outputPath;

		public VotesProcess(string inputPath, string outputPath)
		{
			_inputPath = inputPath;
			_outputPath = outputPath;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SimplePipelineExecutor();
			Register(new XmlRowOperationFile(Path.Combine(_inputPath, "votes.xml")));
			Register(new AddVotesToPost(_outputPath));
		}
	}
}
