// -----------------------------------------------------------------------
//  <copyright file="FileExistsException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.Database.FileSystem.Storage.Exceptions
{
	[Serializable]
	public class FileExistsException : Exception
	{
		public FileExistsException()
		{
		}

		public FileExistsException(string message) : base(message)
		{
		}

		public FileExistsException(string message, Exception inner) : base(message, inner)
		{
		}

		protected FileExistsException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}