define(['durandal/system', 'jquery'], function (system, $) {

	var animationTypes = [
        'bounce',
        'bounceIn',
        'bounceInDown',
        'bounceInLeft',
        'bounceInRight',
        'bounceInUp',
        'bounceOut',
        'bounceOutDown',
        'bounceOutLeft',
        'bounceOutRight',
        'bounceOutUp',
        'fadeIn',
        'fadeInDown',
        'fadeInDownBig',
        'fadeInLeft',
        'fadeInLeftBig',
        'fadeInRight',
        'fadeInRightBight',
        'fadeInUp',
        'fadeInUpBig',
        'fadeOut',
        'fadeOutDown',
        'fadeOutDownBig',
        'fadeOutLeft',
        'fadeOutLeftBig',
        'fadeOutRight',
        'fadeOutRightBig',
        'fadeOutUp',
        'fadeOutUpBig',
        'flash',
        'flip',
        'flipInX',
        'flipInY',
        'flipOutX',
        'flipOutY',
        'hinge',
        'lightSpeedIn',
        'lightSpeedOut',
        'pulse',
        'rollIn',
        'rollOut',
        'rotateIn',
        'rotateInDownLeft',
        'rotateInDownRight',
        'rotateInUpLeft',
        'rotateInUpRight',
        'rotateOut',
        'rotateOutDownLeft',
        'rotateOutDownRight',
        'rotateOutUpLeft',
        'roateOutUpRight',
        'shake',
        'swing',
        'tada',
        'wiggle',
        'wobble'
	];

	function animValue(type) {
		if (Object.prototype.toString.call(type) == '[object String]') {
			return type;
		} else {
			return animationTypes[type];
		}
	}

	function ensureSettings(settings) {
		settings.inAnimation = settings.inAnimation || 'fadeInRight';
		settings.outAnimation = settings.outAnimation || 'fadeOut';
		return settings;
	}

	function doTrans(settings) {
		var parent = settings.parent,
          activeView = settings.activeView,
          newChild = settings.child,
          outAn = animValue(settings.outAnimation),
          inAn = animValue(settings.inAnimation),
          $previousView,
          $newView = $(newChild).removeClass(outAn); // just need to remove outAn here, keeping the animated class so we don't get a "flash"

		return system.defer(function (dfd) {
			if (newChild) {

				$newView = $(newChild);
				if (settings.activeView) {
					outTransition(inTransition);
				} else {
					inTransition();
				}
			}

			function outTransition(callback) {
				$previousView = $(activeView);
				$previousView.addClass('animated')
				$previousView.addClass(outAn);
				setTimeout(function () {
					if (callback) {
						callback();
					}
				}, App.duration);
			}

			function inTransition() {
				if ($previousView) {
					$previousView.css('display', 'none');
				}
				settings.triggerAttach();

				$newView.addClass('animated');  //moved the adding of the animated class here so it keeps it together
				$newView.addClass(inAn);
				$newView.css('display', '');

				setTimeout(function () {
					$newView.removeClass(inAn + ' animated'); // just need to remove inAn here, that's all we'll have
					dfd.resolve(true);
				}, App.duration);
			}

		}).promise();
	}

	return App = {
		duration: 1000 * .35, // seconds
		create: function (settings) {
			settings = ensureSettings(settings);
			return doTrans(settings);
		}
	};
});