import * as EVENTS from "common/constants/events";
import database = require("models/resources/database");
import {
    generateMenuItems,
    menuItem,
    intermediateMenuItem,
    leafMenuItem } from "common/shell/menuItems";

class menu {

    private $mainMenu: JQuery;
    private $mainMenuAnchors: JQuery;
    private $mainMenuLists: JQuery;

    private level: number;
    private type: string = 'menu';

    items: Array<menuItem>;

    routerConfiguration(): Array<DurandalRouteConfiguration> {
        return this.items
            .map(getMenuItemDurandalRoutes)
            .reduce((result, next) => result.concat(next), []);
    }

    static convertToDurandalRoute(leaf: leafMenuItem): DurandalRouteConfiguration {
        return {
            route: leaf.route,
            title: leaf.title,
            moduleId: leaf.moduleId,
            nav: leaf.nav,
            dynamicHash: leaf.hash
        };
    }

    constructor(opts: {
        activeDatabase: KnockoutObservable<database>,
        canExposeConfigOverTheWire: KnockoutObservable<boolean>,
        isGlobalAdmin: KnockoutObservable<boolean>
    }) {
        this.items = generateMenuItems(opts);
    }

    initialize () {
        this.$mainMenu = $('#main-menu');
        this.$mainMenuAnchors = $('#main-menu a');
        this.$mainMenuLists = $('#main-menu ul');

        let self = this;
        this.$mainMenuAnchors.on('click', function (e) {
            var a = this as HTMLAnchorElement;
            var $list = $(a).closest('ul');
            var hasOpenSubmenus = $list.find('.level-show').length;
            var isOpenable = $(a).siblings('.level').length;

            if (!hasOpenSubmenus && isOpenable) {
                $(a).parent().children('.level').addClass('level-show');
                e.stopPropagation();
            }

            self.updateLevel();
        });

        this.$mainMenuLists.on('click', e => {
            e.stopPropagation();

            this.$mainMenuLists
                .find('.level-show')
                .removeClass('level-show');

            this.updateLevel();
        });

        let $body = $('body');
        $('.menu-collapse-button').click(
            () => $body.toggleClass('menu-collapse'));
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
