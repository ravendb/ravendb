require.config({
	paths: {
		'jquery': 'vendor/jquery',
		'backbone': 'vendor/backbone',
		'underscore': 'vendor/underscore',
		'text': 'vendor/text'
	}
});

// This file will be included last, remove jQuery that we loaded earlier
$.noConflict(true);

require(['underscore'], function (_) {
	_.templateSettings = {
		evaluate: /\{\[([\s\S]+?)\]\}/g,
		interpolate: /\{\{([\s\S]+?)\}\}/g
	};
});