export class ResourcesStubs {
    static validValidateName(): Raven.Client.Util.NameValidation {
        return {
            IsValid: true,
            ErrorMessage: null,
        };
    }

    static invalidValidateName(): Raven.Client.Util.NameValidation {
        return {
            IsValid: false,
            ErrorMessage: "Invalid name",
        };
    }
}
