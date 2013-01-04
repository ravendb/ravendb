require.config({
	paths: {
		'jquery': 'vendor/jquery',
		'backbone': 'vendor/backbone',
		'underscore': 'vendor/underscore',
		'text': 'vendor/text'
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