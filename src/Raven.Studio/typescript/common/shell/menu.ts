/// <reference path="../../../typings/tsd.d.ts"/>

import EVENTS = require("common/constants/events");
import router = require("plugins/router");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import generateMenuItems = require("common/shell/menu/generateMenuItems");

class menu {

    private $mainMenu: JQuery;
    private $mainMenuAnchors: JQuery;
    private $mainMenuLevels: JQuery;

    private level: number;
    private type: string = 'menu';

    items: Array<menuItem>;

    private itemsIndex: KnockoutComputed<{ [key: string]: leafMenuItem }>;

    routerConfiguration(): Array<DurandalRouteConfiguration> {
        return this.items
            .map(getMenuItemDurandalRoutes)
            .reduce((result, next) => result.concat(next), [])
            .reduce((result: any[], next: any) => {
                let nextJson = JSON.stringify(next);
                if (!result.some(x => JSON.stringify(x) === nextJson)) {
                    result.push(next);
                }

                return result;
            }, []) as Array<DurandalRouteConfiguration>;
    }

    static convertToDurandalRoute(leaf: leafMenuItem): DurandalRouteConfiguration {
        return {
            route: leaf.route,
            title: leaf.title,
            moduleId: leaf.moduleId,
            nav: leaf.nav,
            dynamicHash: leaf.dynamicHash
        };
    }

    constructor() {
        this.items = generateMenuItems();
        this.itemsIndex = ko.computed(() => this.calculateMenuItemsIndex());
    }

    initialize () {
        this.$mainMenu = $('#main-menu');
        this.$mainMenuAnchors = $('#main-menu a');
        this.$mainMenuLevels = $('#main-menu [data-level]');

        let self: menu = this;
        this.$mainMenuAnchors.on('click', function (e) {
            let a = this as HTMLAnchorElement;
            let $a = $(a);
            if ($a.is('.back')) {
                return handleBack();
            }

            let deepestOpenLevel = self.getDeepestOpenLevelElement();
            if (deepestOpenLevel && $(deepestOpenLevel).find(a).length === 0) {
                return;
            }

            let $list = $a.closest('.level');
            let hasOpenSubmenus = $list.find('.level-show').length;
            let isOpenable = $a.siblings('.level').length;

            if (!hasOpenSubmenus && isOpenable) {
                $a.parent().children('.level').addClass('level-show');
                e.stopPropagation();
            }

            self.updateLevel();

            function handleBack() {
                $a.closest('.level').removeClass('level-show');
                self.updateLevel();
            }
        });

        this.$mainMenuLevels.on('click', function (e) {
            e.stopPropagation();

            let clickedLevelElement = this as HTMLElement;
            let deepestOpenLevelElement = self.getDeepestOpenLevelElement();
            if (clickedLevelElement === deepestOpenLevelElement) {
                return;
            }

            $(clickedLevelElement)
                .find('.level-show')
                .removeClass('level-show');

            self.updateLevel();
        });

        let $body = $('body');
        $('.menu-collapse-button').click(
            () => $body.toggleClass('menu-collapse'));

        router.on('router:navigation:complete', () => {
            this.setActiveMenuItem();
        });

        this.setActiveMenuItem();
    }

    private setActiveMenuItem() {
        let hashToItem = this.itemsIndex();
        let item: leafMenuItem = hashToItem[document.location.hash];

        if (!item) {
            return;
        }

        this.$mainMenu.find('li').removeClass('active');
        this.$mainMenuLevels.removeClass('level-show');

        $(`#main-menu a[href='${item.path()}']`)
            .parent()
            .addClass('active')
            .parents('.level')
            .addClass('level-show');

        this.updateLevel();
    }

    private calculateMenuItemsIndex(): ({ [key: string]: leafMenuItem }) {
        return this.items
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
                return [ item ];
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

    private updateLevel() {
        let newLevel = this.calculateCurrentLevel();
        if (newLevel !== this.level) {
            this.level = newLevel;
            this.emitLevelChanged();
        }

        this.$mainMenu.attr('data-level', this.level);
    }
}

function getMenuItemDurandalRoutes(item: menuItem): Array<DurandalRouteConfiguration> {
    if (item.type === 'intermediate') {
        var intermediateItem = item as intermediateMenuItem;
        return intermediateItem.children
            .map(child => getMenuItemDurandalRoutes(child))
            .reduce((result, next) => result.concat(next), []);
    } else if (item.type === 'leaf') {
        return [ menu.convertToDurandalRoute(item as leafMenuItem) ];
    } 

    return [];
}

export = menu;
