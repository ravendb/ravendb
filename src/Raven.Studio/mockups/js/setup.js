// JavaScript Document

(function (window) {
    var $mainMenu = $('#main-menu');
    var $selectDatabaseContainer = $('.resource-switcher-container');
    var $searchContainer = $('.search-container');

    $('input[type=checkbox]').change(function () {
        var $collapse = $($(this).data('collapse'));
        $collapse.collapse('toggle');
    });

    $selectDatabaseContainer.removeClass('active');
    $searchContainer.removeClass('active');

    function triggerGlobal(evntName, ...args) {
        $(window).trigger(evntName, args);
    }

    (function setupSearch() {
        var $searchInput = $('.search-container input[type=search]');

        $searchInput.click(function (e) {
            show();
            e.stopPropagation();
        });

        $('.search-container .autocomplete-list.box-container')
            .click(e => e.stopPropagation());

        $('.search-container .autocomplete-list.box-container a').on('click', function (e) {
            e.stopPropagation();
            hide();
        });

        $(window)
            .click(hide)
            .on('menu:levelChanged', hide)
            .on('resourceSwitcher:show', hide);

        function show() {
            $searchContainer.addClass('active');
            triggerGlobal('search:show');
        }

        function hide() {
            $searchContainer.removeClass('active');
            triggerGlobal('search:hide');
        }
    }());

    (function setupResourceSwitcher() {

        var $filter = $('.resource-switcher-container .database-filter');

        $selectDatabaseContainer.click(function (e) {
            e.stopPropagation();
            show();
        });

        $('.form-control.btn-toggle.resource-switcher').click(function (e) {
            if ($(this).is('.active')) {
                hide();
            } else {
                show();
            }

            e.stopPropagation();
        });

        $('.resource-switcher-container .box-container a').on('click', function (e) {
            e.stopPropagation();
            hide();
        });

        $(window)
            .click(hide)
            .on('resourceSwitcher:show', function () {
                $filter.focus();
            })
            .on('menu:levelChanged', hide)
            .on('search:show', hide);

        function show() {
            $selectDatabaseContainer.addClass('active');
            triggerGlobal('resourceSwitcher:show');
        }

        function hide() {
            $selectDatabaseContainer.removeClass('active');
            triggerGlobal('resourceSwitcher:hide');
        }
    }());

    (function setupMainMenu() {

        $('#main-menu a').click(function (e) {
            var $list = $(this).closest('ul');
            var hasOpenSubmenus = $list.find('.level-show').length;
            var isOpenable = $(this).siblings('.level').length;

            if (!hasOpenSubmenus && isOpenable) {
                $(this).parent().children('.level').addClass('level-show');
                emitLevelChanged();
                e.stopPropagation();
            }

            setMenuLevelClass();
        });

        $('#main-menu ul').click(function(e) {
            $(this).find('.level-show').removeClass('level-show');
            emitLevelChanged();
            e.stopPropagation();
            setMenuLevelClass();
        });

        $('.menu-collapse-button').click(function() {
            $('body').toggleClass('menu-collapse');
        });

        function emitLevelChanged() {
            triggerGlobal('menu:levelChanged');
        }

        function setMenuLevelClass() {
            var level = $mainMenu.find('.level-show');
            $mainMenu.attr('data-level', level.length);
        }

    }());

    (function setupToggleButtons() {
        $('.btn-toggle').click(function (e) {
            var target = $(this).attr('data-target');
            var targetClass = $(this).attr('data-class');
            $(target).toggleClass(targetClass);
        });
    }());

}( window ));
