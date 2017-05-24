/// <reference path="../../../../typings/tsd.d.ts"/>

class sqlReplicationSimulatedCommand {
    showParamsValues = ko.observable<boolean>(false);
    commandData = ko.observable<commandData>();
    commandText: KnockoutComputed<string>;

    constructor(showParamsValues: boolean, commandData: commandData) {
        this.showParamsValues(showParamsValues);
        this.commandData(commandData);
        this.commandText = ko.computed(()=> {
            if (this.showParamsValues() && !!this.commandData().Params) {
                var processedCommandText = this.commandData().CommandText.slice(0);
                this.commandData().Params.forEach(x => {
                    if (x.Id.slice(0,1) === '@') {
                        processedCommandText = processedCommandText.replace(x.Id, x.Value);
                    } else {
                        processedCommandText = processedCommandText.replace('@' + x.Id, x.Value);
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
