using System.Management.Automation;
using Raven.Client.Document;

namespace Raven.Powershell
{
    [Cmdlet(VerbsCommon.Get, "DatabaseNames")]
    [OutputType(typeof(string[]))]
    public class GetDatabaseNamesCmdlet : Cmdlet
    {
        [Parameter(ValueFromPipelineByPropertyName = true, Mandatory = true, HelpMessage = "Url of RavenDB server, including the port. Example --> http://localhost:8080")]
        public string ServerUrl { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, HelpMessage = "ApiKey to use when connecting to RavenDB Server. It should be full API key. Example --> key1/sAdVA0KLqigQu67Dxj7a")]
        public string ApiKey { get; set; }


        protected override void ProcessRecord()
        {
            using (var store = new DocumentStore
            {
                Url = ServerUrl,                
                ApiKey = ApiKey
            })
            {
                store.Initialize();
                WriteObject(store.DatabaseCommands.GlobalAdmin.GetDatabaseNames(int.MaxValue));
            }
        }
    }
}
