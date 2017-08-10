using Raven.Server.Commercial;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class PutLicenseCommand : PutValueCommand<License>
    {
        public PutLicenseCommand()
        {
            // for deserialization
        }

        public PutLicenseCommand(string name, License license)
        {
            Name = name;
            Value = license;
        }

        public override DynamicJsonValue ValueToJson()
        {
            return Value?.ToJson();
        }
    }
}