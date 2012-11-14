var RavenDBProfiler = (function ($) {
	var options,
	container,
	popupButton,
	resultDialog;

	var templates = ['totals', 'ravendb-profiler', 'session-template', 'request-details'];

	var load = function () {
		if (options.id.length == 0)
			return;

		templates.forEach(function (name) {
			$.get(options.url, { path: "Templates/" + name + ".tmpl.html" }, function (template) {
				$.template(name, template);
			});
		});

		fetchResults(options.id);
	};

	var fetchResults = function (id) {
		$.get(options.url, { id: id }, function (obj) {
			if (obj)
				addResult(obj);
		}, 'json');
	};

	var fixupTableColumnsWidth = function () {
		var firstRow = $('.session-table thead tr').first();
		var cols = $('th', firstRow);
		var maxs = [0, 0, 0, 0, 0, 0, 0];

		$('.session-table tr').each(function (i, row) {
			for (var i = 0; i < cols.length; i++) {
				maxs[i] = Math.max($($('td', row)[i]).width(), maxs[i]);
			}
		});

		$('.session-table tr').each(function (i, row) {
			for (var i = 0; i < cols.length; i++) {
				$($('td', row)[i]).css('min-width', maxs[i]);
			}
		});
	};

	var templateOptions = {
		url: function (str) {
			return str.split('?')[0];
		},
		query: function (str) {
			var results = str.split('?');
			if (results.length > 1) {
				var queryItems = results[1].split('&');
				return unescape(unescape(queryItems.join('\r\n').trim()));
			}
			return "";
		},
		round: function (n) { // round to 2 points after period
			return Math.round(n * 100) / 100;
		},
		totalRequestsTimes: function (data) {
			var total = 0;
			for (var i = 0; i < data.results.length; i++) {
				for (var j = 0; j < data.results[i].Requests.length; j++) {
					total += data.results[i].Requests[j].DurationMilliseconds;
				}
			}
			return this.round(total);
		},
		totalRequests: function (data) {
			var total = 0;
			for (var i = 0; i < data.results.length; i++) {
				total += data.results[i].Requests.length;
			}
			return this.round(total);
		}
	};

	var addResult = function (resultList) {
		if (!popupButton)
			createUI();

		$.tmpl('totals', { results: resultList }, templateOptions).appendTo("#ravendb-session-container");
		$.tmpl('ravendb-profiler', resultList, templateOptions).appendTo("#ravendb-session-container");
		fixupTableColumnsWidth();
	};

	var createUI = function () {
		$.get(options.url, { path: 'styles.css' }, function (result) {
			$('<style>' + result + '</style>').appendTo('body');
		});


		popupButton = $('<span class="rdbprofilerbutton">RavenDB Profiler</span>')
		.appendTo('body')
		.click(function () {
			container.toggle();
		});


		$(document).keyup(function (e) {
			if (e.keyCode == 27) { // esc
				$('.ravendb-close').first().click();
			}
		});

		$(document).delegate('.ravendb-toggle', 'click', function () {
			container.toggle(false);
		});
		$("#ravendb-session-container")
		.delegate('.show-full-url', 'click', function () {
			var item = $.tmplItem(this);
			var req = item.data.Requests[parseInt($(this).attr('request-index'))];
			alert(req.Url);
		})
		.delegate('.show-request-details', 'click', function () {
			var item = $.tmplItem(this);
			var req = item.data.Requests[parseInt($(this).attr('request-index'))];
			$.tmpl('request-details', req, templateOptions).appendTo($('#ravendb-session-container'));

		})
		.delegate('.ravendb-close', 'click', function () {
			$(this).parent().remove();
		})
		.delegate(".toggle-request", "click", function () {

			if (this.collapse) {
				$('.session-information', $(this).parent()).remove();
				this.collapse = false;
				fixupTableColumnsWidth();
				return;
			}

			this.collapse = true;
			var item = $.tmplItem(this);

			$.tmpl('session-template', item.data, templateOptions).appendTo($(this).parent());

			fixupTableColumnsWidth();
		});
	};

	return {
		initalize: function (opt) {
			options = $.extend({}, opt, {});
			container = $('<div class="ravendb-profiler-results" style="display: none;"><div id="ravendb-session-container"/><p/> <a href="#" class="ravendb-toggle ravendb-close">Close</a></div>')
				.appendTo('body');
			createUI();
			$('body').ajaxComplete(function (event, xhrRequest, ajaxOptions) {
				if (ajaxOptions.url.indexOf(options.url) != -1)
					return;
				var id = xhrRequest.getResponseHeader('X-RavenDb-Profiling-Id');
				if (id)
					fetchResults(id.split(', '));
			});
			load();
		}
	};
})(jQuery);