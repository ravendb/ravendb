/// <reference path="../../../../typings/tsd.d.ts"/>

class clientConfigurationModel {
    static readonly readModes = [
        { value: "None", label: "None"},
        { value: "RoundRobin", label: "Round Robin"},
        { value: "FastestNode", label: "Fastest node"}
    ] as Array<valueAndLabelItem<Raven.Client.Http.ReadBalanceBehavior, string>>;

}

export = clientConfigurationModel;
