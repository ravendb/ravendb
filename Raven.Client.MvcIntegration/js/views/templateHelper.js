/*global window*/
define({
	round: function (n) {
		return Math.round(n * 100) / 100; // round to 2 points after period 
	},

	url: function (str) {
		return str.split('?')[0];
	},

	query: function (str) {
		var results = str.split('?'),
			queryItems;
		if (results.length > 1) {
			queryItems = results[1].split('&');
			return window.unescape(window.unescape(queryItems.join('\r\n').trim()));
		}
		return "";
	}
});