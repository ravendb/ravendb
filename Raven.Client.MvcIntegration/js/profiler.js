/*global window*/
define(
	[
		'jquery',
		'underscore',
		'views/ProfilerButton',
		'views/ProfilerView',
		'models/ProfilerData'
	],
	function ($, _, ProfilerButton, ProfilerView, ProfilerData) {

		return function (sessionIds, rootUrl) {
			var profilerData = new ProfilerData({ sessionUrl: rootUrl });
			profilerData.loadSessionData(sessionIds);

			$('head').append($('<link>').attr('rel', 'stylesheet').attr('href', rootUrl + '?path=styles.css'));
			$('body')
				.append(new ProfilerView({ model: profilerData }).render().el)
				.append(new ProfilerButton({ model: profilerData }).render().el);

			if (typeof window.jQuery === 'function') { // bind to original jQuery ajaxComplete
				window.jQuery('body').on('ajaxComplete', _.bind(profilerData.handleResponse, profilerData));
			}
		};
	}
);