import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;

export class ManageServerStubs {
    static getSampleClientGlobalConfiguration(): ClientConfiguration {
        return {
            Disabled: false,
            Etag: 103,
            IdentityPartsSeparator: ".",
            ReadBalanceBehavior: "RoundRobin",
        };
    }
}
