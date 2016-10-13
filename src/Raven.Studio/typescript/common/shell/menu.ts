/// <reference path="../../../typings/tsd.d.ts"/>

import app = require("durandal/app");
import EVENTS = require("common/constants/events");
import router = require("plugins/router");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

class menu {

    activeItem: KnockoutObservable<menuItem> = ko.observable(null);
    deepestOpenItem: KnockoutObservable<intermediateMenuItem> = ko.observable(null);
    level: KnockoutComputed<number> =
        ko.computed(() => {
            let item = this.deepestOpenItem();
            return item ? item.depth() + 1 : 0;
        });

    private $mainMenuLevels: JQuery;

    private type: string = 'menu';
    private items: KnockoutObservable<Array<menuItem>> = ko.observable(null);

    private itemsFlattened: KnockoutComputed<Array<menuItem>> = ko.pureComputed(() => {
        return this.flattenItems(this.items() || []);
    });

    private routeToItemCache: KnockoutComputed<Map<RegExp, leafMenuItem>> = ko.pureComputed(() => {
        return this.itemsFlattened()
            .reduce((result: Map<RegExp, leafMenuItem>, next: menuItem) => {
                if (next.type !== 'leaf') {
                    return result;
                }

                let route = (next as leafMenuItem).route;
                if (typeof(route) === 'string') {
                    cacheItem(route as string, next as leafMenuItem);
                } else if (Array.isArray(route)) {
                    (route as string[]).filter(x => !!x)
                        .forEach(r => cacheItem(r, next as leafMenuItem));
                } else {
                    throw new Error(`Unknown route type: ${ route } ${ typeof(route) }`);
                }

                return result;

                function cacheItem(route: string, item: leafMenuItem) {
                    let regex = routeStringToRegExp(route);
                    if (result.has(regex)) {
                        throw new Error(`Duplicate menu item '${item.title}' for route: ${route}.`);
                    }

                    result.set(regex, item);
                }
            }, new Map<RegExp, leafMenuItem>());
    });

    private registeredRoutes: KnockoutComputed<Array<RegExp>> = ko.computed(() => {
        return Array.from(this.routeToItemCache().keys());
    });

    constructor(items: Array<menuItem>) {
        this.items(items);
    }

    open($data: { item: intermediateMenuItem }, $event: JQueryEventObject) {
        $event.stopPropagation();
        $data.item.isOpen(true);
        this.deepestOpenItem($data.item);
    }

    navigate($data: menuItem, $event: JQueryEventObject) {
        if (this.shouldOpenAsDialog($data)) {
            const leafItem = $data as leafMenuItem;
            require([leafItem.moduleId],
                (viewModel: any) => {
                    app.showDialog(new viewModel);
                });
        } else {
            let a = $event.currentTarget as HTMLAnchorElement;
            router.navigate(a.href);        
        }
    }

    private shouldOpenAsDialog($data: menuItem) {
        return $data instanceof leafMenuItem && ($data as leafMenuItem).openAsDialog;
    }

    back($data: any, $event: JQueryEventObject) {
        $event.stopPropagation();
        $data.item.isOpen(false);
        this.deepestOpenItem($data.item.parent());
        $($event.target)
            .closest('.level')
            .removeClass('level-show');
    }

    update(items: Array<menuItem>) {
        this.items(items);
        this.setActiveMenuItem();
    }

    handleLevelClick($data: any, $event: JQueryEventObject) {
        $event.stopPropagation();

        let $targetLevel = $($event.currentTarget);
        let targetLevelValue = parseInt($targetLevel.attr('data-level'));

        if (this.level() === targetLevelValue) {
            return;
        }

        let itemAtCurrentLevel = this.deepestOpenItem();

        while (this.level() > targetLevelValue) {
            itemAtCurrentLevel.isOpen(false);
            itemAtCurrentLevel = itemAtCurrentLevel.parent() as intermediateMenuItem;
            this.deepestOpenItem(itemAtCurrentLevel);
        }
    }

    initialize() {
        this.$mainMenuLevels = $('#main-menu [data-level]');

        router.on('router:navigation:complete', () => {
            this.setActiveMenuItem();
        });

        this.setActiveMenuItem();
    }

    private setActiveMenuItem() {
        for (let item of this.itemsFlattened()) {
            if (item.type === 'intermediate') {
                (item as intermediateMenuItem).isOpen(false);
            }
        }

        let { fragment } = router.activeInstruction();
        let matchingRoute = this.registeredRoutes()
            .first(routeRegex => routeRegex.test(fragment));

        if (!matchingRoute) {
            return;
        }

        let item = this.routeToItemCache().get(matchingRoute);

        this.activeItem(item);
        this.setLevelToActiveItem();
    }

    private setLevelToActiveItem() {
        let active = this.activeItem();
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

                var resultLevel = this.parseLevel(result);
                var curLevel = this.parseLevel(nextEl);

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
const escapeRegExp = /[\-{}\[\]+?.,\\\^$|#\s]/g;
const routesAreCaseSensitive = false;

function routeStringToRegExp(routeString: string) {
    routeString = routeString.replace(escapeRegExp, '\\$&')
        .replace(optionalParam, '(?:$1)?')
        .replace(namedParam, function (match, optional) {
            return optional ? match : '([^\/]+)';
        })
        .replace(splatParam, '(.*?)');

    return new RegExp('^' + routeString + '$', routesAreCaseSensitive ? undefined : 'i');
}

export = menu;
