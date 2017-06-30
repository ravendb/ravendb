/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class ongoingTaskSubscriptionModel extends ongoingTask {

    editUrl: KnockoutComputed<string>;

    collection = ko.observable<string>();
    clientAddress = ko.observable<string>();
    connectedFrom = ko.observable<string>();
    sendDocumetnsFromChangeVector = ko.observable<string>();
    lastSentChangeVector = ko.observable<string>();
    script = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Server.Web.System.OngoingTaskSubscription) {
        super();
        this.update(dto);
        this.initializeObservables();
        this.initValidation();
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editSubscription(this.taskId);
    }

    update(dto: Raven.Server.Web.System.OngoingTaskSubscription) {
        super.update(dto);

/*  TODO
        this.collection(dto.Collection);
        this.clientAddress(dto.ClientAddress);
        this.connectedFrom(dto.ConnectedFrom);
        this.sendDocumetnsFromChangeVector(dto.SendDocumetnsFromChangeVector);
        this.lastSentChangeVector(dto.LastSentChangeVector);
        this.script(dto.Script);
*/        
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toDto(): subscriptionDataFromUI {
        return {
            // TODO...
            TaskName: this.taskName()
        };
    }

    initValidation() {
        super.initValidation();

        // TODO: add extend() to observables....

        this.validationGroup = ko.validatedObservable({
            // TODO: add relevent properties..
            taskName: this.taskName
        });
    }

    static empty(): ongoingTaskSubscriptionModel {

        return new ongoingTaskSubscriptionModel({
            TaskName: "",
            TaskType: "Subscription"
        } as Raven.Server.Web.System.OngoingTaskSubscription);
    }
}

export = ongoingTaskSubscriptionModel;
