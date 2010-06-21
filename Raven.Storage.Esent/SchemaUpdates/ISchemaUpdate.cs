using System.ComponentModel.Composition;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent.SchemaUpdates
{
	[InheritedExport]
	public interface ISchemaUpdate
	{
		string FromSchemaVersion { get;  }
		void Update(Session session, JET_DBID dbid);
	}
}