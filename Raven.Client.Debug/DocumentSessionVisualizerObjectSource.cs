using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.VisualStudio.DebuggerVisualizers;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;

namespace Raven.Client.Debug
{
	public class DocumentSessionVisualizerObjectSource : VisualizerObjectSource
	{
		public override void GetData(object target, System.IO.Stream outgoingData)
		{
			var session = (DocumentSession) target;
			var profilingInformation = ((DocumentStore) session.DocumentStore).GetProfilingInformationFor(session.Id) ??
			                           ProfilingInformation.CreateProfilingInformation(session.Id);
			new BinaryFormatter().Serialize(outgoingData, profilingInformation);
		}

		public override object CreateReplacementObject(object target, System.IO.Stream incomingData)
		{
			return new BinaryFormatter().Deserialize(incomingData);
		}
	}
}