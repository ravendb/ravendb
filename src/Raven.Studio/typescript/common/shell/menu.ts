/// <reference path="../../../typings/tsd.d.ts"/>

import EVENTS = require("common/constants/events");
import router = require("plugins/router");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

class menu {

    private $mainMenu: JQuery;
    private $mainMenuAnchors: JQuery;
    private $mainMenuLevels: JQuery;

    private type: string = 'menu';
    private items: KnockoutObservable<Array<menuItem>> = ko.observable(null);
    private itemsIndex: KnockoutComputed<{ [key: string]: leafMenuItem }>;

    activeItem: KnockoutObservable<menuItem> = ko.observable(null);
    deepestOpenItem: KnockoutObservable<intermediateMenuItem> = ko.observable(null);
    level: KnockoutComputed<number> =
        ko.computed(() => {
            let item = this.deepestOpenItem();
            return item ? item.depth() + 1 : 0;
        });

    constructor(items: Array<menuItem>) {
        this.items(items);
        this.itemsIndex = ko.computed(() => this.calculateMenuItemsIndex());
    }

    open($data: { item: intermediateMenuItem }, $event: JQueryEventObject) {
        $event.stopPropagation();
        $data.item.isOpen(true);
        this.deepestOpenItem($data.item);
    }

    navigate($data: { item: menuItem }, $event: JQueryEventObject) {
        let a = $event.currentTarget as HTMLAnchorElement;
        router.navigate(a.href);
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
        this.$mainMenu = $('#main-menu');
        this.$mainMenuAnchors = $('#main-menu a');
        this.$mainMenuLevels = $('#main-menu [data-level]');

        router.on('router:navigation:complete', () => {
            this.setActiveMenuItem();
        });

        this.setActiveMenuItem();
    }

    private setActiveMenuItem() {
        for (let item of this.getItemsFlattened()) {
            if (item.type === 'intermediate') {
                (item as intermediateMenuItem).isOpen(false);
            }
        }

        let hashToItem = this.itemsIndex();
        let item: leafMenuItem = hashToItem[document.location.hash];

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

    private calculateMenuItemsIndex(): ({ [key: string]: leafMenuItem }) {
        return this.items()
            .reduce((result: menuItem[], next: menuItem) =>
                result.concat(flatten(next)), [] as menuItem[])
            .filter((x: leafMenuItem) => !!x.path())
            .reduce((result: { [key: string]: leafMenuItem }, next: leafMenuItem) => {
                result[next.path()] = next;
                return result;
            }, {} as { [key: string]: leafMenuItem });

        function flatten(item: menuItem): Array<menuItem> {
            if (item.type === 'intermediate') {
                return (item as intermediateMenuItem).children
                    .map(flatten)
                    .reduce((result, child) => result.concat(child), []);
            } else if (item.type === 'leaf') {
                return [item];
            }

            return [];
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

    private calculateCurrentLevel() {
        return this.$mainMenu.find('.level-show').length;
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

export = menu;
