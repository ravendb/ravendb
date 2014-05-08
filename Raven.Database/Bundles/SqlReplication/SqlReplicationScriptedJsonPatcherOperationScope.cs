using Jint.Native;

using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.SqlReplication
{
	public class SqlReplicationScriptedJsonPatcherOperationScope : DefaultScriptedJsonPatcherOperationScope
	{
		public SqlReplicationScriptedJsonPatcherOperationScope(DocumentDatabase database)
			: base(database)
		{
		}

		public override RavenJObject ConvertReturnValue(JsValue jsObject)
		{
			return null;// we don't use / need the return value
		}
	}
}