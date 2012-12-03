require.config({
	paths: {
		'jquery': '../Scripts/jquery-1.8.2',
		'backbone': '../Scripts/backbone',
		'underscore': '../Scripts/underscore',
		'text': '../Scripts/text'
	},
	shim: {
		underscore: {
			init: function () {
				return this._.noConflict();
			}
		},
		backbone: {
			deps: ['underscore', 'jquery'],
			init: function () {
				return this.Backbone.noConflict();
			}
		}
	}
});

require(['jquery'], function ($) {
	$.noConflict(true); // restore global $ and jQuery to original values
});

require(['underscore'], function (_) {
	_.templateSettings = {
		evaluate: /\{\[([\s\S]+?)\]\}/g,
		interpolate: /\{\{([\s\S]+?)\}\}/g
	};
});