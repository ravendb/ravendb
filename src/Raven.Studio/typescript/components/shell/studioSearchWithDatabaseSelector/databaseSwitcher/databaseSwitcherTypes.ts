export interface DatabaseSwitcherOption {
    value: string;
    isSharded: boolean;
    environment: Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
    isDisabled: boolean;
}
