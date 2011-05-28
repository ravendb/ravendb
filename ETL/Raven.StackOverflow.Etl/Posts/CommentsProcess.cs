//-----------------------------------------------------------------------
// <copyright file="CommentsProcess.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Database;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Posts
{
	public class CommentsProcess : EtlProcess
	{
		private readonly string _inputDirectory;
		private readonly string _outputDirectory;

		public CommentsProcess(string inputDirectory, string outputDirectory)
		{
			_inputDirectory = inputDirectory;
			_outputDirectory = outputDirectory;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SingleThreadedPipelineExecuter();
			Register(new XmlRowOperationFile(Path.Combine(_inputDirectory, "comments.xml")));
			Register(new AddCommentsToPost(_outputDirectory));
		}
	}
}
