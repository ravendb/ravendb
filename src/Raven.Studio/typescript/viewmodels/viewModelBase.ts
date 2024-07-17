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
import messagePublisher = require("common/messagePublisher");
import { jwerty } from "jwerty";

/*
 * Base view model class that provides basic view model services, such as tracking the active database and providing a means to add keyboard shortcuts.
*/
abstract class viewModelBase {

    protected get activeDatabase() {
        return activeDatabaseTracker.default.database;
    }
    
    protected isReadOnlyAccess =  ko.pureComputed(() => accessManager.default.readOnlyOrAboveForDatabase(this.activeDatabase()));
    protected isReadWriteAccessOrAbove = ko.pureComputed(() => accessManager.default.readWriteAccessOrAboveForDatabase(this.activeDatabase()));
    protected isAdminAccessOrAbove = ko.pureComputed(() => accessManager.default.adminAccessOrAboveForDatabase(this.activeDatabase()));
    
    protected isOperatorOrAbove = accessManager.default.isOperatorOrAbove;
    protected isClusterAdminOrClusterNode = accessManager.default.isClusterAdminOrClusterNode;
    
    abstract view: { default: string };
    
    getView() {
        if (!this.view) {
            throw new Error("Looks like you forgot to define view in: " + this.constructor.name);
        }
        if (!this.view.default.trim().startsWith("<")) {
            console.warn("View doesn't start with '<'");
        }
        return this.view.default || this.view;
    }
    
    downloader = new downloader();

    isBusy = ko.observable<boolean>(false);
    
    private keyboardShortcutDomContainers: string[] = [];
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

    customDiscardStayResult = ko.observable<() => JQueryDeferred<confirmDialogResult>>(null);

    constructor() {
        this.appUrls = appUrl.forCurrentDatabase();

        eventsCollector.default.reportViewModel();
    }
    
    protected bindToCurrentInstance(...methods: Array<keyof this & string>) {
        _.bindAll(this, ...methods);
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    canActivate(args: any): boolean | JQueryPromise<canActivateResultDto> {
        setTimeout(() => viewModelBase.showSplash(this.isAttached === false), 700);
        this.downloader.reset();

        const dbNameFromUrl = appUrl.getDatabaseNameFromUrl();
        const canAccessView = this.canAccessView(dbNameFromUrl);
          
        if (!canAccessView) {
            const task = $.Deferred<canActivateResultDto>();
            messagePublisher.reportError("Access is forbidden. Redirecting to databases view.");
            task.resolve({ redirect: appUrl.forDatabases() });
            return task;
        }
        
        return databasesManager.default.activateBasedOnCurrentUrl(dbNameFromUrl);
    }

    protected canAccessView(dbName?: string): boolean {
        const requiredAccessForView = router.activeInstruction().config.requiredAccess;
        if (!requiredAccessForView) {
            return true;
        }
        
        return accessManager.canHandleOperation(requiredAccessForView, dbName);
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

        this.updateHelpLink(null); // clean link
    }

    getPageHostDimenensions(): [number, number] {
        return viewHelpers.getPageHostDimenensions();
    }

    attached() {
        window.addEventListener("beforeunload", this.beforeUnloadListener);
        this.isAttached = true;
        viewModelBase.showSplash(false);
        
        const bs5 = this.isUsingBootstrap5();
        if (bs5 != null) {
            const pageHostRoot = $("#page-host-root");
            if (bs5) {
                pageHostRoot.removeClass("bs3");
                pageHostRoot.addClass("bs5");
            } else {
                pageHostRoot.removeClass("bs5");
                pageHostRoot.addClass("bs3");
            }
        }
    }

    /**
     * returns information if given view is using bootstrap 5
     * true/false - self descriptive
     * undefined - view can't determinate that and ambient style should be used 
     *              this is mainly to cover confirmation dialogs etc. 
     */
    isUsingBootstrap5(): true | false | undefined {
        return false;
    }

    compositionComplete() {
        this.dirtyFlag().reset(); //Resync Changes
    }
   
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    canDeactivate(isClose: boolean): boolean | JQueryPromise<canDeactivateResultDto> {
        if (this.dirtyFlag().isDirty()) {
            return this.customDiscardStayResult()?.() ?? this.discardStayResult();
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

        this.disposableActions.forEach(f => f.dispose());
        this.disposableActions = [];

        this.isAttached = true;
        viewModelBase.showSplash(false);
        this.disposed = true;
    }

    protected registerDisposableDelegateHandler($element: JQuery, event: string, delegate: string, handler: (event: any) => void) {
        $element.on(event as any, delegate, handler);

        this.disposableActions.push({
             dispose: () => $element.off(event as any, delegate, handler)
        });
    }
    
    protected registerDisposableHandler($element: JQuery<any>, event: string, handler: (event: any) => void) {
        $element.on(event as any, handler);

        this.disposableActions.push({
             dispose: () => $element.off(event as any, handler)
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
