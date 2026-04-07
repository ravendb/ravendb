class prefixPathModel {
    path = ko.observable<string>();
    validationGroup: KnockoutValidationGroup;

    static normalize(path: string): string {
        if (path == null) {
            return path;
        }

        return path.trim();
    }

    constructor(prefixPath: string) {

        this.path(prefixPath);
        this.initValidation();
    }

    initValidation() {
        this.path.extend({
            validation: [
                {
                    validator: () => {
                        const normalizedPath = prefixPathModel.normalize(this.path());

                        if (normalizedPath) {
                            const pathLength = normalizedPath.length;
                            if (pathLength === 1) {
                                return true;
                            }
                            
                            const lastChar = normalizedPath.charAt(pathLength - 1);
                            const prevChar = normalizedPath.charAt(pathLength - 2);
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
