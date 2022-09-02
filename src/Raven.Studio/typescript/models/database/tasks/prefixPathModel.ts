class prefixPathModel {
    path = ko.observable<string>();
    validationGroup: KnockoutValidationGroup;

    constructor(prefixPath: string) {

        this.path(prefixPath);
        this.initValidation();
    }

    initValidation() {
        this.path.extend({
            validation: [
                {
                    validator: () => {
                        if (this.path()) {
                            const pathLength = this.path().length;
                            if (pathLength === 1) {
                                return true;
                            }
                            
                            const lastChar = this.path().charAt(pathLength - 1);
                            const prevChar = this.path().charAt(pathLength - 2);
                            return lastChar != '*' || prevChar === '/' || prevChar === '-';
                        }
                        
                        return true;
                    },
                    message: "When using '*' as the last character, the previous character must be '/' or '-'"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            path: this.path
        });
    }
}

export = prefixPathModel;
