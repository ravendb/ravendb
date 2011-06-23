//-----------------------------------------------------------------------
// <copyright file="PostsProcess.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.StackOverflow.Etl.Generic;
using Rhino.Etl.Core;
using Rhino.Etl.Core.Pipelines;

namespace Raven.StackOverflow.Etl.Posts
{
	public class PostsProcess : EtlProcess
	{
		private readonly string inputPath;
		private readonly string _outputPath;

		public PostsProcess(string inputPath, string outputPath)
		{
			this.inputPath = inputPath;
			_outputPath = outputPath;
		}

		protected override void Initialize()
		{
			PipelineExecuter = new SimplePipelineExecutor();
			Register(new XmlRowOperationFile(Path.Combine(inputPath, "posts.xml")));
			Register(new RowToDatabase("Posts", doc => "posts/" + doc["Id"], _outputPath));
		}
	}
}
