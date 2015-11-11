using System;
using Raven.Json.Linq;

namespace Raven.Abstractions.Streaming
{
	public interface IOutputWriter : IDisposable
	{
		string ContentType { get; }

		void WriteHeader();
		void Write(RavenJObject result);
		void WriteError(Exception exception);
		void Flush();
	}
}
