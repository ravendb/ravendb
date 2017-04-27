/// <reference path="../../typings/tsd.d.ts"/>

import appUrl = require("common/appUrl");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import router = require("plugins/router");
import changeSubscription = require("common/changeSubscription");
import oauthContext = require("common/oauthContext");
import changesContext = require("common/changesContext");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import document = require("models/database/documents/document");
import downloader = require("common/downloader");
import databasesManager = require("common/shell/databasesManager");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");
import eventsCollector = require("common/eventsCollector");
import viewHelpers = require("common/helpers/view/viewHelpers");

/*
 * Base view model class that provides basic view model services, such as tracking the active database and providing a means to add keyboard shortcuts.
*/
class viewModelBase {

    protected activeDatabase = activeDatabaseTracker.default.database;
    
    downloader = new downloader();

    isBusy = ko.observable<boolean>(false);

    protected databasesManager = databasesManager.default;

    private keyboardShortcutDomContainers: string[] = [];
    static modelPollingHandle: number; // mark as static to fix https://github.com/BlueSpire/Durandal/issues/181
    private notifications: Array<changeSubscription> = [];
    private disposableActions: Array<disposable> = [];
    appUrls: computedAppUrls;
    private postboxSubscriptions: Array<KnockoutSubscription> = [];
    static showSplash = ko.observable<boolean>(false);
    private isAttached = false;

    pluralize = pluralizeHelpers.pluralize;

    protected changesContext = changesContext.default;
    
    dirtyFlag = new ko.DirtyFlag([]);

    currentHelpLink = ko.observable<string>().subscribeTo('globalHelpLink', true);


    //holds full studio version eg. 4.0.40000
    static clientVersion = ko.observable<string>();
    static hasContinueTestOption = ko.observable<boolean>(false);

    constructor() {
        this.appUrls = appUrl.forCurrentDatabase();

        eventsCollector.default.reportViewModel(this);
    }

    protected bindToCurrentInstance(...methods: Array<keyof this>) {
        _.bindAll(this, ...methods);
    }

    canActivate(args: any): boolean | JQueryPromise<canActivateResultDto> {
        var self = this;
        setTimeout(() => viewModelBase.showSplash(self.isAttached === false), 700);
        this.downloader.reset();

        return this.databasesManager.activateBasedOnCurrentUrl();
    }

    activate(args: any, isShell = false) {
        // create this ko.computed once to avoid creation and subscribing every 50 ms - thus creating memory leak.
        const adminArea = this.appUrls.isAreaActive("admin");

        oauthContext.enterApiKeyTask.done(() => {
            if (isShell || adminArea())
                return;

            this.changesContext
                .afterChangesApiConnected(() => this.afterClientApiConnected());
        });

        this.postboxSubscriptions = this.createPostboxSubscriptions();
        this.modelPollingStart();


        ko.postbox.publish("SetRawJSONUrl", "");
        this.updateHelpLink(null); // clean link
    }

    getPageHostDimenensions(): [number, number] {
        const $pageHostRoot = $(".dynamic-container");

        return [$pageHostRoot.innerWidth(), $pageHostRoot.innerHeight()];
    }

    attached() {
        window.addEventListener("beforeunload", this.beforeUnloadListener, false);
        this.isAttached = true;
        viewModelBase.showSplash(false);
    }

    compositionComplete() {        
        this.dirtyFlag().reset(); //Resync Changes
    }

    canDeactivate(isClose: boolean): any {
        const isDirty = this.dirtyFlag().isDirty();
        if (isDirty) {
            const discard = "Discard changes";
            const stay = "Stay on this page";
            const discardStayResult = $.Deferred();
            const confirmation = this.confirmationMessage("Unsaved changes", "You have unsaved changes. How do you want to proceed?", [discard, stay], true);
            confirmation.done((result: { can: boolean; }) => {
                if (!result.can) {
                    this.dirtyFlag().reset();    
                }
                result.can = !result.can;
                discardStayResult.resolve(result);
            });

            return discardStayResult;
        }

        return true;
    }

    detached() {
        this.currentHelpLink.unsubscribeFrom("globalHelpLink");
        this.cleanupNotifications();
        this.cleanupPostboxSubscriptions();

        window.removeEventListener("beforeunload", this.beforeUnloadListener, false);

        this.isAttached = true;
        viewModelBase.showSplash(false);
    }

    deactivate() {
        this.keyboardShortcutDomContainers.forEach(el => this.removeKeyboardShortcuts(el));
        this.modelPollingStop();

        this.disposableActions.forEach(f => f.dispose());
        this.disposableActions = [];

        this.isAttached = true;
        viewModelBase.showSplash(false);
    }

    protected registerDisposableHandler($element: JQuery, event: string, handler: Function) {
        $element.on(event as any, handler);

        this.disposableActions.push({
             dispose: () => $element.off(event as any, handler as any)
        });
    }

    protected registerDisposable(disposable: disposable) {
        this.disposableActions.push(disposable);
    }

    protected afterClientApiConnected(): void {
        // empty here
    }

    cleanupNotifications() {
        this.notifications.forEach((notification: changeSubscription) => notification.off());
        this.notifications = [];
    }

    addNotification(subscription: changeSubscription) {
        this.notifications.push(subscription);
    }

    removeNotification(subscription: changeSubscription) {
        _.pull(this.notifications, subscription);
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [];
    }

    cleanupPostboxSubscriptions() {
        this.postboxSubscriptions.forEach((subscription: KnockoutSubscription) => subscription.dispose());
        this.postboxSubscriptions = [];
    }

    /*
     * Creates a keyboard shortcut local to the specified element and its children.
     * The shortcut will be removed as soon as the view model is deactivated.
     * Also defines shortcut for ace editor, if ace editor was received
     */
    createKeyboardShortcut(keys: string, handler: () => void, elementSelector: string) {
        jwerty.key(keys, e => {
            e.preventDefault();
            handler();
        }, this, elementSelector);

        if (!_.includes(this.keyboardShortcutDomContainers, elementSelector)) {
            this.keyboardShortcutDomContainers.push(elementSelector);
        }
    }

    private removeKeyboardShortcuts(elementSelector: string) {
        $(elementSelector).unbind('keydown.jwerty');
    }

    /*
     * Navigates to the specified URL, recording a navigation event in the browser's history.
     */
    navigate(url: string) {
        router.navigate(url);
    }

    /*
     * Navigates by replacing the current URL. It does not record a new entry in the browser's navigation history.
     */
    updateUrl(url: string) {
        const options: DurandalNavigationOptions = {
            replace: true,
            trigger: false
        };
        router.navigate(url, options);
    }

    modelPolling(): JQueryPromise<any> {
        return null;
    }

    pollingWithContinuation() {
        const poolPromise = this.modelPolling();
        if (poolPromise) {
            poolPromise.always(() => {
                viewModelBase.modelPollingHandle = setTimeout(() => {
                    viewModelBase.modelPollingHandle = null;
                    this.pollingWithContinuation();
                    }, 5000);
            });
        }
    }

    modelPollingStart() {
        // clear previous pooling handle (if any)
        if (viewModelBase.modelPollingHandle) {
            this.modelPollingStop();
        }
        this.pollingWithContinuation();
    }

    modelPollingStop() {
        clearTimeout(viewModelBase.modelPollingHandle);
        viewModelBase.modelPollingHandle = null;
    }

    protected confirmationMessage(title: string, confirmationMessage: string, options: string[] = ["No", "Yes"], forceRejectWithResolve: boolean = false): JQueryPromise<confirmDialogResult> {
        return viewHelpers.confirmationMessage(title, confirmationMessage, options, forceRejectWithResolve);
    }

    canContinueIfNotDirty(title: string, confirmationMessage: string) {
        var deferred = $.Deferred<void>();

        const isDirty = this.dirtyFlag().isDirty();
        if (isDirty) {
            this.confirmationMessage(title, confirmationMessage)
                .done((result) => {
                    if (result.can) {
                        deferred.resolve();
                    } else {
                        deferred.reject();
                    }
                });
        } else {
            deferred.resolve();
        }

        return deferred;
    }

    protected setupDisableReasons() {
        $('.has-disable-reason').tooltip({
            container: "body"
        });
    }

    private beforeUnloadListener: EventListener = (e: any): any => {
        var isDirty = this.dirtyFlag().isDirty();
        if (isDirty) {
            const message = "You have unsaved data.";
            e = e || window.event;

            // For IE and Firefox
            if (e) {
                e.returnValue = message;
            }

            // For Safari
            return message;
        }
    }

    continueTest() {
        const doc = document.empty();
        new saveDocumentCommand("Debug/Done", doc, this.activeDatabase(), false)
            .execute()
            .done(() => viewModelBase.hasContinueTestOption(false));
    }

    updateHelpLink(hash: string = null) {
        if (hash) {
            var version = viewModelBase.clientVersion();
            if (version) {
                var href = "http://ravendb.net/l/" + hash + "/" + version + "/";
                ko.postbox.publish('globalHelpLink', href);
                return;
            }

            var subscribtion = viewModelBase.clientVersion.subscribe(v => {
                var href = "http://ravendb.net/l/" + hash + "/" + v + "/";
                ko.postbox.publish('globalHelpLink', href);

                if (subscribtion) {
                    subscribtion.dispose();
                }
            });
        } else {
            ko.postbox.publish('globalHelpLink', null);
        }
    }

    protected afterAsyncValidationCompleted(context: KnockoutValidationGroup, callback: Function) {
        const validationGroup = (context as any)();
        const keys = _.keys(validationGroup);

        const asyncValidations = [] as Array<KnockoutObservable<boolean>>;

        keys.forEach(key => {
            if ("isValidating" in validationGroup[key]) {
                asyncValidations.push(validationGroup[key].isValidating);
            }
        });

        if (asyncValidations.length === 0 || _.every(asyncValidations, x => !x())) {
            callback();
            return;
        }

        // there are any validations in progress, await them

        let subscriptions = [] as Array<KnockoutSubscription>;

        const onUpdate = () => {
            if (_.every(asyncValidations, x => !x())) {
                // all validators completed its work, clean up and call callback
                subscriptions.forEach(x => x.dispose());
                callback();
            }
        }

        subscriptions = asyncValidations.map(v => v.subscribe(() => onUpdate()));
    }

    protected isValid(context: KnockoutValidationGroup, showErrors = true): boolean {
        return viewHelpers.isValid(context, showErrors);
    }

}

export = viewModelBase;
