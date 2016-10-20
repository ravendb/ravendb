import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import counterChange = require("models/counter/counterChange");
import counter = require("models/counter/counter");
import counterSummary = require("models/counter/counterSummary");
import editCounterDialog = require("viewmodels/counter/editCounterDialog");
import getCounterCommand = require("commands/counter/getCounterCommand");
import updateCounterCommand = require("commands/counter/updateCounterCommand");
import resetCounterCommand = require("commands/counter/resetCounterCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteItems = require("viewmodels/common/deleteItems");

class editCounter extends viewModelBase {

    groupName = ko.observable<string>();
    counterName = ko.observable<string>();
    groupLink = ko.observable<string>();
    counter = ko.observable<counter>();
    isLoading = ko.observable<boolean>();
    topRecentCounters = ko.computed(() => this.getTopRecentCounters());
    isBusy = ko.observable(false);

    static container = "#editCounterContainer";
    static recentCountersInCounterStorage = ko.observableArray<{ counterStorageName: string; recentCounters: KnockoutObservableArray<IGroupAndCounterName> }>();

    constructor() {
        super();
    }

    canActivate(args: any) {
        super.canActivate(args);

        var deffered = $.Deferred();
        if (!args.groupName || !args.counterName) {
            messagePublisher.reportError("Can't find group name or counter name in query string!");
            deffered.resolve({ redirect: appUrl.forCounterStorageCounters(null, this.activeCounterStorage()) });
        }

        var cs = this.activeCounterStorage();
        this.load(args.groupName, args.counterName)
            .done(() => {
                this.groupName(args.groupName);
                this.counterName(args.counterName);
                this.groupLink(appUrl.forCounterStorageCounters(args.groupName, cs));
                this.appendRecentCounter(args.groupName, args.counterName);
                deffered.resolve({ can: true });
            })
            .fail(() => {
                messagePublisher.reportError("Can't find counter!");
                this.removeFromTopRecentCounters(args.groupName, args.counterName);
                deffered.resolve({ redirect: appUrl.forCounterStorageCounters(null, cs) });
            });

        return deffered;
    }

    attached() {
        super.attached();
        this.setupKeyboardShortcuts();
    }

    setupKeyboardShortcuts() {
        this.createKeyboardShortcut("alt+shift+del", () => this.deleteCounter(), editCounter.container);
    }

    load(groupName: string, counterName: string) {
        this.isLoading(true);
        return new getCounterCommand(this.activeCounterStorage(), groupName, counterName)
            .execute()
            .done((result: counter) => {
                this.counter(result);
                this.isLoading(false);
            });
    }

    refresh() {
        this.load(this.groupName(), this.counterName());
    }

    change() {
        var dto = {
            CurrentValue: this.counter().total(),
            Group: this.groupName(),
            CounterName: this.counterName(),
            Delta: 0
        };
        var change = new counterChange(dto);
        var counterChangeVm = new editCounterDialog(change);
        counterChangeVm.updateTask.done((change: counterChange, isNew: boolean) => {
            var counterCommand = new updateCounterCommand(this.activeCounterStorage(), change.group(), change.counterName(), change.delta(), isNew);
            var execute = counterCommand.execute();
            execute.done(() => this.refresh());
        });
        app.showDialog(counterChangeVm);
    }

    reset() {
        var confirmation = this.confirmationMessage("Reset Counter", "Are you sure that you want to reset the counter?");
        confirmation.done(() => {
            var resetCommand = new resetCounterCommand(this.activeCounterStorage(), this.groupName(), this.counterName());
            var execute = resetCommand.execute();
            execute.done(() => this.refresh());
        });
    }

    deleteCounter() {
        var summary: counterSummary = new counterSummary({
            GroupName: this.groupName(),
            CounterName: this.counterName(),
            Total: this.counter().total()
        });
        var viewModel = new deleteItems([summary], this.activeCounterStorage());
        viewModel.deletionTask.done(() => {
            var countersUrl = appUrl.forCounterStorageCounters(null, this.activeCounterStorage());
            router.navigate(countersUrl);
        });
        app.showDialog(viewModel, editCounter.container);
    }

    removeFromTopRecentCounters(groupName: string, counterName: string) {
        var currentFilesystemName = this.activeFilesystem().name;
        var recentFilesForCurFilesystem = editCounter.recentCountersInCounterStorage().first(x => x.counterStorageName === currentFilesystemName);
        if (recentFilesForCurFilesystem) {
            var counter = {
                groupName: groupName,
                counterName: counterName
            }
            recentFilesForCurFilesystem.recentCounters.remove(counter);
        }
    }

    getTopRecentCounters() {
        var cs = this.activeCounterStorage();
        var recentFilesForCurFilesystem = editCounter.recentCountersInCounterStorage().first(x => x.counterStorageName === cs.name);
        if (recentFilesForCurFilesystem) {
            var value = recentFilesForCurFilesystem
                .recentCounters()
                .filter((x: IGroupAndCounterName) => {
                    return x.groupName !== this.groupName() && x.counterName !== this.counterName();
                })
                .slice(0, 5)
                .map((groupAndCounterName: IGroupAndCounterName) => {
                    var groupName = groupAndCounterName.groupName;
                    var counterName = groupAndCounterName.counterName;
                    return {
                        groupName: groupName,
                        counterName: counterName,
                        counterUrl: appUrl.forEditCounter(cs, groupName, counterName)
                    };
                });
            return value;
        } else {
            return [];
        }
    }

    appendRecentCounter(groupName: string, counterName: string) {
        var csName = this.activeCounterStorage().name;
        var existingRecentCounters = editCounter.recentCountersInCounterStorage.first(x => x.counterStorageName === csName);
        if (existingRecentCounters) {
            var existingCounter = existingRecentCounters.recentCounters.first((x: IGroupAndCounterName) => x.groupName === groupName && x.counterName === counterName);
            if (!existingCounter) {
                if (existingRecentCounters.recentCounters().length === 5) {
                    existingRecentCounters.recentCounters.pop();
                }
                existingRecentCounters.recentCounters.unshift({groupName: groupName, counterName: counterName});
            }
        } else {
            editCounter.recentCountersInCounterStorage.push({ counterStorageName: csName, recentCounters: ko.observableArray([{ groupName: groupName, counterName: counterName }]) });
        }
    }
}

export = editCounter;
