export class SharedStubs {
    static nodeConnectionTestErrorResult(): Raven.Server.Web.System.NodeConnectionTestResult {
        return {
            Success: false,
            HTTPSuccess: false,
            TcpServerUrl: null,
            Log: [],
            Error: "System.UriFormatException: Invalid URI: The format of the URI could not be determined.\n   at System.Uri.CreateThis(String uri, Boolean dontEscape, UriKind uriKind, UriCreationOptions& creationOptions)\n   at System.Uri..ctor(String uriString)\n   at Raven.Server.Documents.ETL.Providers.Queue.QueueBrokerConnectionHelper.CreateRabbitMqConnection(RabbitMqConnectionSettings settings) in D:\\Builds\\RavenDB-6.0-Nightly\\20231123-0200\\src\\Raven.Server\\Documents\\ETL\\Providers\\Queue\\QueueBrokerConnectionHelper.cs:line 80",
        };
    }

    static nodeConnectionTestSuccessResult(): Raven.Server.Web.System.NodeConnectionTestResult {
        return {
            Success: true,
            HTTPSuccess: true,
            TcpServerUrl: null,
            Log: [],
            Error: null,
        };
    }
}
