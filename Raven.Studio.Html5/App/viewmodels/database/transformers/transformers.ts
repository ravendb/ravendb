import viewModelBase = require("viewmodels/viewModelBase");
import transformer = require("models/database/index/transformer");
import getTransformersCommand = require("commands/database/transformers/getTransformersCommand");
import appUrl = require("common/appUrl");
import deleteTransformerConfirm = require("viewmodels/database/transformers/deleteTransformerConfirm");
import app = require("durandal/app");
import changeSubscription = require("common/changeSubscription");
import shell = require("viewmodels/shell");
import copyTransformerDialog = require("viewmodels/database/transformers/copyTransformerDialog");

class Transformers extends viewModelBase {

    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;
    appUrls: computedAppUrls;
    transformersGroups = ko.observableArray<{ entityName: string; transformers: KnockoutObservableArray<transformer> }>();
    containerSelector = "#transformersContainer";
    transformersMutex = true;
    allTransformersExpanded = ko.observable(true);
    expandCollapseTitle = ko.computed(() => this.allTransformersExpanded() ? "Collapse all" : "Expand all");


    constructor() {
        super();

        this.appUrls = appUrl.forCurrentDatabase();
    }

    canActivate(args: any): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            this.fetchTransformers(db).done(() => deferred.resolve({ can: true }));
        }
        return deferred;
    }

    attached() {
        this.updateHelpLink('OWRJLV');
        this.createKeyboardShortcut("Alt+N", () => this.navigate(this.newTransformerUrl()), this.containerSelector);
        ko.postbox.publish("SetRawJSONUrl", appUrl.forTransformersRawData(this.activeDatabase()));
    }

    private fetchTransformers(db) {
        return new getTransformersCommand(db)
            .execute()
            .done((transformers: transformerDto[]) => {
                transformers
                    .map(curTransformer=> new transformer().initFromLoad(curTransformer))
                    .forEach(i=> this.putTransformerIntoGroups(i));
            });
    }

    createNotifications(): Array<changeSubscription> {
        return [shell.currentResourceChangesApi().watchAllTransformers((e: transformerChangeNotificationDto) => this.processTransformerEvent(e))];
    }

    private processTransformerEvent(e: transformerChangeNotificationDto) {
        if (e.Type == "TransformerRemoved") {
            this.removeTransformersFromAllGroups(this.findTransformersByName(e.Name));
        } else {
            if (this.transformersMutex == true) {
                this.transformersMutex = false;
                setTimeout(() => {
                    this.fetchTransformers(this.activeDatabase()).always(() => this.transformersMutex = true);
                }, 5000);
            }
        }
    }

    findTransformersByName(transformerName: string) {
        var result = new Array<transformer>();
        this.transformersGroups().forEach(g => {
            g.transformers().forEach(i => {
                if (i.name() == transformerName) {
                    result.push(i);
                }
            });
        });

        return result;
    }

    putTransformerIntoGroups(trans: transformer) {
        
        var groupName = trans.name().split("/")[0];
        var group = this.transformersGroups.first(g=> g.entityName === groupName);

        if (group) {
            var existingTrans = group.transformers.first((cur: transformer)=> cur.name() == trans.name());

            if (!existingTrans) {
                group.transformers.push(trans);
            }
        } else {
            this.transformersGroups.push({ entityName: groupName, transformers: ko.observableArray([trans]) });
        }
    }

    toggleExpandAll() {
        $(".index-group-content").collapse("toggle");
        this.allTransformersExpanded.toggle();
    }

    deleteAllTransformers() {
        var allTransformers: transformer[];
        allTransformers = this.getAllTransformers();
        this.promptDeleteTransformers(allTransformers);
    }

    getAllTransformers(): transformer[] {
        var all: transformer[] = [];
        this.transformersGroups().forEach(g => all.pushAll(g.transformers()));
        return all.distinct();
    }

    deleteTransformer(transformerToDelete: transformer) {
        this.promptDeleteTransformers([transformerToDelete]);
    }

    pasteTransformer() {
        app.showDialog(new copyTransformerDialog('', this.activeDatabase(), true));
    }

    copyTransformer(t: transformer) {
        app.showDialog(new copyTransformerDialog(t.name(), this.activeDatabase(), false));
    }

    private promptDeleteTransformers(transformers: Array<transformer>) {
        var db = this.activeDatabase();
        var deleteViewmodel = new deleteTransformerConfirm(transformers.map(i => i.name()), db);
        deleteViewmodel.deleteTask.done(() => this.removeTransformersFromAllGroups(transformers));
        app.showDialog(deleteViewmodel);
    }

    private removeTransformersFromAllGroups(transformers: Array<transformer>) {
        this.transformersGroups().forEach(transGroup => transGroup.transformers.removeAll(transformers));
        this.transformersGroups.remove((item: { entityName: string; transformers: KnockoutObservableArray<transformer> }) => item.transformers().length === 0);
    }
}

export = Transformers;