/*global jQuery, Handlebars */
jQuery(function ($) {
  'use strict';

  var Utils = {
    pluralize: function (count, word) {
      return count === 1 ? word : word + 's';
    },
  };

  var App = {
    init: function () {
      this.ENTER_KEY = 13;
      this.cacheElements();
      this.bindEvents();
      this.render();
    },
    cacheElements: function () {
      this.todoTemplate = Handlebars.compile($('#todo-template').html());
      this.footerTemplate = Handlebars.compile($('#footer-template').html());
      this.$todoApp = $('#todoapp');
      this.$header = this.$todoApp.find('#header');
      this.$main = this.$todoApp.find('#main');
      this.$footer = this.$todoApp.find('#footer');
      this.$newTodo = this.$header.find('#new-todo');
      this.$searchTodo = this.$header.find('#search-todo');
      this.$clearSearch = this.$header.find('#clear-search');
      this.$toggleAll = this.$main.find('#toggle-all');
      this.$todoList = this.$main.find('#todo-list');
      this.$count = this.$footer.find('#todo-count');
      this.$clearBtn = this.$footer.find('#clear-completed');

    },
    bindEvents: function () {
      var list = this.$todoList;
      this.$newTodo.on('keyup', this.create);
      this.$searchTodo.on('keyup', this.search);
      this.$clearSearch.on('click', this.clearSearch);
      this.$toggleAll.on('change', this.toggleAll);
      this.$footer.on('click', '#clear-completed', this.destroyCompleted);
      list.on('change', '.toggle', this.toggle);
      list.on('dblclick', 'label', this.edit);
      list.on('keypress', '.edit', this.blurOnEnter);
      list.on('blur', '.edit', this.update);
      list.on('click', '.destroy', this.destroy);
    },
    render: function () {
      var that = this;

      $.ajax({
        type: 'GET',
        async: false,
        url: "jsondata.json",
        data: {
          search: App.$searchTodo.val()
        },
        complete : function(xhr) {
            var items = JSON.parse(xhr.responseText);
            that.$todoList.html(that.todoTemplate(items));
            that.$main.toggle(!!items.length);
            that.$toggleAll.prop('checked', !that.activeTodoCount(items));
            that.renderFooter(items);
        }
      });

    },
    renderFooter: function (items) {
      var todoCount = items.length;
      var activeTodoCount = this.activeTodoCount(items);
      var footer = {
        activeTodoCount: activeTodoCount,
        activeTodoWord: Utils.pluralize(activeTodoCount, 'item'),
        completedTodos: todoCount - activeTodoCount
      };

      this.$footer.toggle(!!todoCount);
      this.$footer.html(this.footerTemplate(footer));
    },
    toggleAll: function () {
      var completed = $(this).prop('checked');
      var paramList = '';

      $(".chk-list").each(function(){
        paramList += '&id=' + $(this).data('id');
      });

      $.ajax({
          type: 'PUT',
          url: 'jsondata.json?&completed=' + completed + paramList,
          async: false,
          complete : function(r) {
              App.render();
          }
      });

    },
    activeTodoCount: function (items) {
      var count = 0;

      $.each(items, function (i, val) {
        if (!val.Completed) {
          count++;
        }
      });

      return count;
    },
    destroyCompleted: function () {
      var paramList = '';

        $(".completed").each(function(){
          paramList += '&id=' + $(this).data('id');
        });

        $.ajax({
            type: 'DELETE',
            url: 'jsondata.json?' + paramList,
            async: false,
            complete : function(r) {
                App.render();
            }
        });
    },
    create: function (e) {
      var $input = $(this);
      var val = $.trim($input.val());

      if (e.which !== App.ENTER_KEY || !val) {
        return;
      }

      $.ajax({
        type: 'POST',
        url: "jsondata.json",
        async: false,
        data: {
          title: val
        },
        complete : function(r) {
            $input.val('');
            App.render();
        }
      });

    },
    search: function (e) {
        var $input = $(this);
        var val = $.trim($input.val());

        if (e.which !== App.ENTER_KEY) {
          return;
        }

        App.render();

      },
     clearSearch: function (e) {
          App.$searchTodo.val('');
          App.render();

        },
    toggle: function () {
        var completed = $(this).is(':checked');
        var id = $(this).closest('li').data('id');
        $.ajax({
            type: 'PUT',
            url: "jsondata.json?id=" + id + '&completed=' + completed,
            async: false,
            complete : function(r) {
                App.render();
            }
        });
    },
    edit: function () {
      var $input = $(this).closest('li').addClass('editing').find('.edit');
      var val = $input.val();

      $input.val(val).focus();
    },
    blurOnEnter: function (e) {
      if (e.which === App.ENTER_KEY) {
        e.target.blur();
      }
    },
    update: function () {
      var val = $.trim($(this).removeClass('editing').val());
      var id = $(this).closest('li').data('id');
      $.ajax({
          type: 'PUT',
          url: "jsondata.json?id=" + id + '&title=' + val,
          async: false,
          complete : function(r) {
              App.render();
          }
      });
    },
    destroy: function () {
      var id = $(this).closest('li').data('id');

      $.ajax({
          type: 'DELETE',
          url: "jsondata.json?id=" + id + '&id=9999&p2=test',
          async: false,
          complete : function(r) {
              App.render();
          }
      });
    }
  };

  App.init();
});
