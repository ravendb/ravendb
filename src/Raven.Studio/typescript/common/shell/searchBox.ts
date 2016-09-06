
import EVENTS = require("common/constants/events");

/*
    Events emitted through ko.postbox
        * SearchBox.Show - when searchbox is opened
        * SearchBox.Hide - when searchbox is hidden
        * SearchBox.Input - on searchbox input, passes the searchbox value as arg
*/
class searchBox {

    private $searchContainer: JQuery;
    private $searchInput: JQuery;

    initialize() {
        this.$searchInput = $('.search-container input[type=search]');
        this.$searchContainer = $('.search-container');

        this.$searchInput.on('input', (e) => {
            ko.postbox.publish('SearchBox.Input', this.$searchInput.val());
        });

        this.$searchInput.click((e) => {
            e.stopPropagation();
            this.show();
        });

        $('.search-container .autocomplete-list.box-container')
            .click(e => e.stopPropagation());

        $('.search-container .autocomplete-list.box-container a').on('click', (e) => {
            e.stopPropagation();
            this.hide();
        });

        var hide = () => this.hide();
        $(window).on('click', hide);
        ko.postbox.subscribe(EVENTS.Menu.LevelChanged, hide);
        ko.postbox.subscribe(EVENTS.ResourceSwitcher.Show, hide);
    }

    private show() {
        this.$searchContainer.addClass('active');
        ko.postbox.publish(EVENTS.SearchBox.Show);
    }

    private hide() {
        this.$searchContainer.removeClass('active');
        ko.postbox.publish(EVENTS.SearchBox.Hide);
    }
}

export = searchBox;