/// <reference path="../../../../typings/tsd.d.ts"/>

class sqlReplicationSimulatedCommand {
    showParamsValues = ko.observable<boolean>(false);
    commandData = ko.observable<commandData>();
    commandText: KnockoutComputed<string>;

    constructor(showParamsValues, commandData) {
        this.showParamsValues(showParamsValues);
        this.commandData(commandData);
        this.commandText = ko.computed(()=> {
            if (this.showParamsValues() && !!this.commandData().Params) {
                var processedCommandText = this.commandData().CommandText.slice(0);
                this.commandData().Params.forEach(x => {
                    if (x.Key.slice(0,1) === '@') {
                        processedCommandText = processedCommandText.replace(x.Key, x.Value);
                    } else {
                        processedCommandText = processedCommandText.replace('@' + x.Key, x.Value);
                    }
                });
                return processedCommandText;
            } else {
                return this.commandData().CommandText;
            }
        });
    }
}

export = sqlReplicationSimulatedCommand;
