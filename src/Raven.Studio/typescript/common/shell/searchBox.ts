
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
    private readonly hideHandler = (e: Event) => {
        if (this.shouldConsumeHideEvent(e)) {
            this.hide()
        }
    };

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
    }

    private show() {
        window.addEventListener("click", this.hideHandler, true);

        this.$searchContainer.addClass('active');
    }

    private hide() {
        window.removeEventListener("click", this.hideHandler, true);

        this.$searchContainer.removeClass('active');
    }

    private shouldConsumeHideEvent(e: Event) {
        return $(e.target).parents(".search-container").length === 0;
    }
}

export = searchBox;