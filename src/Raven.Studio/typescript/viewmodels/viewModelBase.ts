/// <reference path="../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import router = require("plugins/router");
import changeSubscription = require("common/changeSubscription");
import changesContext = require("common/changesContext");
import downloader = require("common/downloader");
import databasesManager = require("common/shell/databasesManager");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");
import eventsCollector = require("common/eventsCollector");
import viewHelpers = require("common/helpers/view/viewHelpers");
import accessManager = require("common/shell/accessManager");

/*
 * Base view model class that provides basic view model services, such as tracking the active database and providing a means to add keyboard shortcuts.
*/
class viewModelBase {

    protected activeDatabase = activeDatabaseTracker.default.database;
    
    protected isReadOnlyAccess = accessManager.default.isReadOnlyAccess;
    protected isReadWriteAccessOrAbove = accessManager.default.isReadWriteAccessOrAbove;
    protected isAdminAccessOrAbove = accessManager.default.isAdminAccessOrAbove;
    
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
    protected disposed = false;

    pluralize = pluralizeHelpers.pluralize;

    protected changesContext = changesContext.default;
    
    dirtyFlag = new ko.DirtyFlag([]);

    currentHelpLink = ko.observable<string>().subscribeTo('globalHelpLink', true);

    //holds full studio version eg. 4.0.40000
    static clientVersion = ko.observable<string>();

    constructor() {
        this.appUrls = appUrl.forCurrentDatabase();

        eventsCollector.default.reportViewModel(this);
    }

    protected bindToCurrentInstance(...methods: Array<keyof this & string>) {
        _.bindAll(this, ...methods);
    }

    canActivate(args: any): boolean | JQueryPromise<canActivateResultDto> {
        setTimeout(() => viewModelBase.showSplash(this.isAttached === false), 700);
        this.downloader.reset();

        return this.databasesManager.activateBasedOnCurrentUrl();
    }

    activate(args: any, parameters?: any) {
        // create this ko.computed once to avoid creation and subscribing every 50 ms - thus creating memory leak.
        const adminArea = this.appUrls.isAreaActive("admin");

        const isShell = parameters && parameters.shell;
        
        if (!isShell && !adminArea()) {
            this.changesContext
                .afterChangesApiConnected(() => this.afterClientApiConnected());
        }

        this.postboxSubscriptions = this.createPostboxSubscriptions();
        this.modelPollingStart();

        this.updateHelpLink(null); // clean link
    }

    getPageHostDimenensions(): [number, number] {
        return viewHelpers.getPageHostDimenensions();
    }

    attached() {
        window.addEventListener("beforeunload", this.beforeUnloadListener);
        this.isAttached = true;
        viewModelBase.showSplash(false);
    }

    compositionComplete() {        
        this.dirtyFlag().reset(); //Resync Changes
    }
   
    canDeactivate(isClose: boolean): boolean | JQueryPromise<canDeactivateResultDto> {
        if (this.dirtyFlag().isDirty()) {
            return this.discardStayResult();
        }

        return true;
    }

    discardStayResult() {
        const discard = "Discard changes";
        const stay = "Stay on this page";
        const discardStayResult = $.Deferred<confirmDialogResult>();
        const confirmation = this.confirmationMessage("Unsaved changes", "You have unsaved changes. How do you want to proceed?", {
            buttons: [discard, stay],
            forceRejectWithResolve: true
        });

        confirmation.done((result: confirmDialogResult) => {
            if (!result.can) {
                this.dirtyFlag().reset(); 
            }
            result.can = !result.can;
            discardStayResult.resolve(result);
        });

        return discardStayResult;
    }

    detached() {
        this.currentHelpLink.unsubscribeFrom("globalHelpLink");
        this.cleanupNotifications();
        this.cleanupPostboxSubscriptions();

        window.removeEventListener("beforeunload", this.beforeUnloadListener);

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
        this.disposed = true;
    }

    protected registerDisposableDelegateHandler($element: JQuery, event: string, delegate: string, handler: Function) {
        $element.on(event as any, delegate, handler);

        this.disposableActions.push({
             dispose: () => $element.off(event as any, delegate, handler as any)
        });
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

    protected confirmationMessage(title: string, confirmationMessage: string, options?: confirmationDialogOptions): JQueryPromise<confirmDialogResult> {
        return viewHelpers.confirmationMessage(title, confirmationMessage, options);
    }

    canContinueIfNotDirty(title: string, confirmationMessage: string) {
        const deferred = $.Deferred<void>();

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

    private beforeUnloadListener: EventListener = (e: BeforeUnloadEvent): string => {
        const isDirty = this.dirtyFlag().isDirty();
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
    };

    updateHelpLink(hash: string = null) {
        if (hash) {
            const version = viewModelBase.clientVersion();
            if (version) {
                const href = "http://ravendb.net/l/" + hash + "/" + version + "/";
                ko.postbox.publish('globalHelpLink', href);
            } else {
                const subscription = viewModelBase.clientVersion.subscribe(v => {
                    const href = "http://ravendb.net/l/" + hash + "/" + v + "/";
                    ko.postbox.publish('globalHelpLink', href);

                    if (subscription) {
                        subscription.dispose();
                    }
                });
            }
        } else {
            ko.postbox.publish('globalHelpLink', null);
        }
    }

    protected isValid(context: KnockoutValidationGroup, showErrors = true): boolean {
        return viewHelpers.isValid(context, showErrors);
    }
}

export = viewModelBase;
