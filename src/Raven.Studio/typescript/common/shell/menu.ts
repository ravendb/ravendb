/// <reference path="../../../typings/tsd.d.ts"/>

import EVENTS = require("common/constants/events");
import router = require("plugins/router");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import studioSettings = require("common/settings/studioSettings");

class menu {

    static readonly minWidth = 220;
    static readonly maxWidth = 500;
    
    activeItem: KnockoutObservable<menuItem> = ko.observable(null);
    deepestOpenItem: KnockoutObservable<intermediateMenuItem> = ko.observable(null);
    level: KnockoutComputed<number> =
        ko.pureComputed(() => {
            const item = this.deepestOpenItem();
            return item ? item.depth() + 1 : 0;
        });

    private $mainMenuLevels: JQuery;
    private menuElement: HTMLElement;
    private menuResizeElement: HTMLElement;
    width = ko.observable<number>(280);

    private type = 'menu';
    public items: KnockoutObservable<Array<menuItem>> = ko.observable(null);

    private resetMenuLevelToActiveItem: {
        isActive: boolean,
        windowClickHandler: (e?: MouseEvent) => void;
        menuClickHandler: (e?: MouseEvent) => void;
    };

    private itemsFlattened: KnockoutComputed<Array<menuItem>> = ko.pureComputed(() => {
        return this.flattenItems(this.items() || []);
    });

    private routeToItemCache: KnockoutComputed<Map<RegExp, leafMenuItem>> = ko.pureComputed(() => {
        return this.itemsFlattened()
            .reduce((result: Map<RegExp, leafMenuItem>, next: menuItem) => {
                if (next.type !== 'leaf') {
                    return result;
                }
                
                const leafItem = (next as leafMenuItem);
                
                if (leafItem.alias) {
                    return result;
                }

                const route = (next as leafMenuItem).route;
                if (typeof(route) === 'string') {
                    cacheItem(route, next as leafMenuItem);
                } else if (Array.isArray(route)) {
                    route.filter(x => !!x)
                        .forEach(r => cacheItem(r, next as leafMenuItem));
                } else {
                    throw new Error(`Unknown route type: ${ route } ${ typeof(route) }`);
                }

                return result;

                function cacheItem(route: string, item: leafMenuItem) {
                    const regex = routeStringToRegExp(route);
                    if (result.has(regex)) {
                        throw new Error(`Duplicate menu item '${item.title}' for route: ${route}.`);
                    }

                    result.set(regex, item);
                }
            }, new Map<RegExp, leafMenuItem>());
    });

    private registeredRoutes: KnockoutComputed<Array<RegExp>> = ko.pureComputed(() => {
        return Array.from(this.routeToItemCache().keys());
    });

    private onUpdate() {
        this.setActiveMenuItem();

        $('#main-menu [data-toggle="tooltip"]').tooltip({
            placement: "right",
            container: ".main-menu",
            html: true
        });
    }

    constructor(items: KnockoutObservable<menuItem[]>) {
        this.items = items;

        this.items.subscribe(() => this.onUpdate());
    }

    handleIntermediateItemClick($data: { item: intermediateMenuItem }, $event: JQuery.TriggeredEvent) {
        const { item } = $data;
        if (item.isOpen()) {
            item.close();
            return;
        }

        const currentItem = this.deepestOpenItem();
        if (currentItem && currentItem !== item) {
            currentItem.close();
        }

        this.open(item, $event);

        this.toggleResetLevelBehavior(item);
    }

    open(item: intermediateMenuItem, $event: JQuery.TriggeredEvent) {
        $event.stopPropagation();
        item.open();
        this.deepestOpenItem(item);

        if (item.path()) {
            if ($event.ctrlKey) {
                window.open(item.path());
            } else {
                router.navigate(item.path());
            }
        }
    }

    navigate($data: menuItem, $event: JQuery.TriggeredEvent) {
        const targetLink = $event.target.closest("a");
        if (targetLink && targetLink.classList.contains("disabled")) {
            $event.preventDefault();
            return false;
        }

        const a = $event.currentTarget as HTMLAnchorElement;

        if ($event.ctrlKey) {
            window.open(a.href);
        } else {
            router.navigate(a.href);
        }
    }

    back($data: any, $event: JQuery.TriggeredEvent) {
        const { item } = $data;
        $event.stopPropagation();
        item.isOpen(false);
        this.deepestOpenItem(item.parent());
        $($event.target)
            .closest('.level')
            .removeClass('level-show');
    }

    handleLevelClick($data: any, $event: JQuery.TriggeredEvent) {
        $event.stopPropagation();

        const $targetLevel = $($event.currentTarget);
        const targetLevelValue = parseInt($targetLevel.attr('data-level'));

        if (this.level() === targetLevelValue) {
            return;
        }

        this.closeOpenLevels(targetLevelValue);
    }

    initialize() {
        this.$mainMenuLevels = $('#main-menu [data-level]');
        this.menuElement = document.getElementById("main-menu");
        this.menuResizeElement = document.getElementById("resizeArea");

        router.on('router:navigation:complete', () => {
            this.setActiveMenuItem();
        });

        this.setActiveMenuItem();
        this.initializeOnClickOutsideOfMenuResetLevelToActiveItem();

        $(this.menuResizeElement).on("mousedown.menuResize", e => this.handleResize(e));

        studioSettings.default.globalSettings()
            .done(settings => {
                const widthFromSettings = settings.menuWidth.getValue();
                this.width(widthFromSettings);
                document.documentElement.style.setProperty('--menu-width', widthFromSettings.toString() + 'px');
            });
    }
    
    private handleResize(e: JQuery.TriggeredEvent) {

        const $document = $(document);
        
        const startX = e.pageX;
        const currentWidth = this.width();

        $document.on("mousemove.menuResize", e => {
            const dx = e.pageX - startX;
            const requestedWidth = currentWidth + dx;
            
            this.width(_.clamp(requestedWidth, menu.minWidth, menu.maxWidth));
            document.documentElement.style.setProperty('--menu-width', (_.clamp(currentWidth + dx, menu.minWidth, menu.maxWidth).toString() + 'px'));
        });

        $document.on("mouseup.menuResize", e => {
            const dx = e.pageX - startX;
            const requestedWidth = _.clamp(currentWidth + dx, menu.minWidth, menu.maxWidth);
            const width = _.clamp(requestedWidth, menu.minWidth, menu.maxWidth);
            this.width(width);
            document.documentElement.style.setProperty('--menu-width', requestedWidth.toString() + 'px');

            studioSettings.default.globalSettings()
                .done(settings => {
                    settings.menuWidth.setValue(this.width());
                });

            $document.off("mousemove.menuResize");
            $document.off("mouseup.menuResize");
        });
    }
    
    private toggleResetLevelBehavior(item: intermediateMenuItem) {
        const activeItem = this.activeItem();
        if (!activeItem) {
            return;
        }

        const activeItemParent = activeItem.parent();
        this.toggleResetLevelToActiveItem(activeItemParent !== item);
    }

    private initializeOnClickOutsideOfMenuResetLevelToActiveItem() {
        let scheduleResetTimer: ReturnType<typeof setTimeout> = null;

        const menuClickHandler = () => {
            if (scheduleResetTimer) {
                clearTimeout(scheduleResetTimer);
                scheduleResetTimer = null;
            }
        };

        const windowClickHandler = () => {
            scheduleResetTimer = setTimeout(() => {
                    this.closeOpenLevels();
                    this.setLevelToActiveItem();
                }, 0);
        };

        this.resetMenuLevelToActiveItem = {
            isActive: false,
            menuClickHandler,
            windowClickHandler
        };
    }

    private toggleResetLevelToActiveItem(toggle: boolean) {
        const opts = this.resetMenuLevelToActiveItem;
        const alreadyActive = opts.isActive;

        if (toggle && !alreadyActive) {
            opts.isActive = true;
            window.addEventListener('click', opts.windowClickHandler, true);
            this.menuElement.addEventListener('click', opts.menuClickHandler, true);
        } else if (!toggle && alreadyActive) {
            opts.isActive = false;
            window.removeEventListener('click', opts.windowClickHandler, true);
            this.menuElement.removeEventListener('click', opts.menuClickHandler, true);
        }
    }

    private closeOpenLevels(targetLevel = 0) {
        let itemAtCurrentLevel = this.deepestOpenItem();

        while (this.level() > targetLevel) {
            itemAtCurrentLevel.isOpen(false);
            itemAtCurrentLevel = itemAtCurrentLevel.parent() as intermediateMenuItem;
            this.deepestOpenItem(itemAtCurrentLevel);
        }
    }

    private setActiveMenuItem() {
        const flattenedItems = this.itemsFlattened();
        for (let i = 0; i < flattenedItems.length; i++) {
            const item = flattenedItems[i];
            if (item.type === 'intermediate') {
                (item as intermediateMenuItem).isOpen(false);
            }
        }

        if (!router.activeInstruction()) {
            return;
        }

        const { fragment } = router.activeInstruction();
        const matchingRoute = this.registeredRoutes()
            .find(routeRegex => routeRegex.test(fragment));

        if (!matchingRoute) {
            return;
        }

        const itemMatchingRoute = this.routeToItemCache().get(matchingRoute);
        if (itemMatchingRoute.itemRouteToHighlight) {
                // Highlight/Activate a different menu item
                const matchingRoute = this.registeredRoutes()
                    .find(routeRegex => routeRegex.test(itemMatchingRoute.itemRouteToHighlight));
                const itemToActivate = this.routeToItemCache().get(matchingRoute);
                this.activeItem(itemToActivate);
        } else {
            this.activeItem(itemMatchingRoute);
        }

        this.setLevelToActiveItem();
    }    

    private setLevelToActiveItem() {
        const active = this.activeItem();
        if (!active) {
            return;
        }

        let current = active.parent() as intermediateMenuItem;
        this.deepestOpenItem(current);
        while (current) {
            current.isOpen(true);
            current = current.parent() as intermediateMenuItem;
        }
    }

    private getDeepestOpenLevelElement() {
        return this.$mainMenuLevels.find('.level-show')
            .toArray()
            .reduce((result: HTMLElement, nextEl: HTMLElement) => {
                if (!result) {
                    return nextEl;
                }

                const resultLevel = this.parseLevel(result);
                const curLevel = this.parseLevel(nextEl);

                if (resultLevel > curLevel) {
                    return result;
                }

                return nextEl;
            }, null);
    }

    private parseLevel(el: HTMLElement) {
        return parseInt(el.dataset['level']);
    }

    private emitLevelChanged() {
        ko.postbox.publish(EVENTS.Menu.LevelChanged, this.level);
    }

    private getItemsFlattened() {
        return this.flattenItems(this.items());
    }

    private flattenItems(items: menuItem[]) {
         return items.reduce((result: menuItem[], next: menuItem) => {
                addToResult(result, next);
                return result;
            }, []);

        function addToResult(result: menuItem[], item: menuItem) {
            if (item.type === 'intermediate') {
                (item as intermediateMenuItem).children
                    .forEach(x => addToResult(result, x));
            }

            result.push(item);
        }
    }
}

/*
    The following code has been extracted from durandal router plugin since it was private - I needed the logic transforming the route into a regular expression. Here's the original location: https://github.com/BlueSpire/Durandal/blob/master/src/plugins/js/router.js#L23
*/
const optionalParam = /\((.*?)\)/g;
const namedParam = /(\(\?)?:\w+/g;
const splatParam = /\*\w+/g;
const escapeRegExp = /[-{}[\]+?.,\\^$|#\s]/g;
const routesAreCaseSensitive = false;

function routeStringToRegExp(routeString: string) {
    routeString = routeString.replace(escapeRegExp, '\\$&')
        .replace(optionalParam, '(?:$1)?')
        .replace(namedParam, function (match, optional) {
            return optional ? match : '([^/]+)';
        })
        .replace(splatParam, '(.*?)');

    return new RegExp('^' + routeString + '$', routesAreCaseSensitive ? undefined : 'i');
}

export = menu;
